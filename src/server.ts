import { spawn } from "node:child_process";
import { mkdirSync, openSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";

type Where = { url: string; home: string };

async function jetstreamReady(url: string): Promise<boolean> {
  try {
    const nc = await connect({ servers: url, reconnect: false, timeout: 500 });
    try {
      await (await jetstreamManager(nc)).getAccountInfo();
      return true;
    } finally {
      await nc.close();
    }
  } catch {
    return false;
  }
}

export function findServerBin(env = process.env): string {
  const bin = env.AMB_NATS_SERVER_BIN || Bun.which("nats-server");
  if (!bin) throw new Error("nats-server not found on PATH or AMB_NATS_SERVER_BIN; install it with: brew install nats-server");
  return bin;
}

/** Starts a detached, localhost-only nats-server unless one already answers. The port bind is the lock. */
export async function ensureServer({ url, home }: Where): Promise<"running" | "started"> {
  if (await jetstreamReady(url)) return "running";
  const port = new URL(url).port || "4222";
  mkdirSync(home, { recursive: true });
  const log = openSync(join(home, "nats.log"), "a");
  const child = spawn(
    findServerBin(),
    ["-js", "-sd", join(home, "jetstream"), "-a", "127.0.0.1", "-p", port, "-P", join(home, "nats.pid"), "-n", "amb"],
    { detached: true, stdio: ["ignore", log, log] },
  );
  child.unref();
  for (const deadline = Date.now() + 5000; Date.now() < deadline; await Bun.sleep(100)) {
    // A losing racer exits on bind failure; the winner's server still satisfies us.
    if (await jetstreamReady(url)) return "started";
  }
  throw new Error(`nats-server did not become ready on ${url}; see ${join(home, "nats.log")}`);
}

const argsOf = (pid: number) => Bun.spawnSync(["ps", "-p", String(pid), "-o", "args="]).stdout.toString().trim();

/** Whether a command line is a nats-server whose store dir is exactly this home's. */
function servesHome(args: string, home: string): boolean {
  const t = args.split(/\s+/);
  return /(^|\/)nats-server$/.test(t[0] ?? "") && t.some((x, i) => x === "-sd" && t[i + 1] === join(home, "jetstream"));
}

export type StopResult = "stopped" | "no-pid-file" | "not-ours" | "stale";

/** Stops this home's detached server. A stale pid file is removed; a pid naming any other process is left alone. */
export function stopServer({ home }: Pick<Where, "home">): StopResult {
  const pidFile = join(home, "nats.pid");
  let pid: number;
  try {
    pid = Number(readFileSync(pidFile, "utf8").trim());
  } catch {
    return "no-pid-file";
  }
  const kill = (p: number) => {
    try {
      process.kill(p, "SIGTERM");
      return true;
    } catch {
      return false;
    }
  };
  const args = Number.isInteger(pid) && pid > 0 ? argsOf(pid) : "";
  if (args) {
    if (!servesHome(args, home) || !kill(pid)) return "not-ours";
    rmSync(pidFile, { force: true });
    return "stopped";
  }
  // The recorded pid is dead. Our server may still run under another pid (pid file rewritten or lost).
  rmSync(pidFile, { force: true });
  const others = Bun.spawnSync(["pgrep", "-f", "--", `-sd ${join(home, "jetstream")}`]).stdout.toString().split(/\s+/).filter(Boolean).map(Number);
  const ours = others.filter((p) => servesHome(argsOf(p), home));
  return ours.length && ours.every(kill) ? "stopped" : "stale";
}

export const serverStatus = ({ url }: Pick<Where, "url">) => jetstreamReady(url);
