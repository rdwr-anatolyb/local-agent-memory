import { spawn } from "node:child_process";
import { mkdirSync, openSync, readFileSync, rmSync } from "node:fs";
import { join, win32 } from "node:path";
import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";

type Where = { url: string; home: string };

/** Injected seams for the platform-specific paths. Each one defaults to the real call. */
export type Deps = {
  platform?: NodeJS.Platform;
  run?: (cmd: string[]) => string;
  kill?: (pid: number, sig: string) => void;
  spawn?: typeof spawn;
  which?: (name: string) => string | null;
  ready?: (url: string) => Promise<boolean>;
};
type Resolved = Required<Deps>;

const withDefaults = (deps: Deps = {}): Resolved => ({
  platform: process.platform,
  run: (cmd) => Bun.spawnSync(cmd).stdout.toString(),
  kill: (pid, sig) => process.kill(pid, sig as NodeJS.Signals),
  spawn,
  which: (name) => Bun.which(name),
  ready: jetstreamReady,
  ...deps,
});

/** The install line for a missing nats-server. Windows must not be told to use brew. */
function installHint(platform: NodeJS.Platform): string {
  const head = "nats-server not found on PATH or AMB_NATS_SERVER_BIN";
  return platform === "win32"
    ? `${head}; download nats-server.exe from https://github.com/nats-io/nats-server/releases and set AMB_NATS_SERVER_BIN to its full path`
    : `${head}; install it with: brew install nats-server`;
}

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

export function findServerBin(env = process.env, deps?: Deps): string {
  const d = withDefaults(deps);
  const bin = env.AMB_NATS_SERVER_BIN || d.which("nats-server") || (d.platform === "win32" ? d.which("nats-server.exe") : null);
  if (!bin) throw new Error(installHint(d.platform));
  return bin;
}

/** Starts a detached, localhost-only nats-server unless one already answers. The port bind is the lock. */
/** Detached and unref'd so the server outlives the starter. windowsHide is win32-only, so darwin/linux options stay as they were. */
function spawnDetached(bin: string, args: string[], log: number, d: Resolved) {
  const child = d.spawn(bin, args, {
    detached: true,
    stdio: ["ignore", log, log],
    ...(d.platform === "win32" ? { windowsHide: true } : {}),
  });
  child.unref();
  return child;
}

export async function ensureServer({ url, home }: Where, deps?: Deps): Promise<"running" | "started"> {
  const d = withDefaults(deps);
  if (await d.ready(url)) return "running";
  const port = new URL(url).port || "4222";
  mkdirSync(home, { recursive: true });
  const log = openSync(join(home, "nats.log"), "a");
  const pj = joinFor(d.platform);
  spawnDetached(
    findServerBin(process.env, d),
    ["-js", "-sd", pj(home, "jetstream"), "-a", "127.0.0.1", "-p", port, "-P", pj(home, "nats.pid"), "-n", "amb"],
    log,
    d,
  );
  for (const deadline = Date.now() + 5000; Date.now() < deadline; await Bun.sleep(100)) {
    // A losing racer exits on bind failure; the winner's server still satisfies us.
    if (await d.ready(url)) return "started";
  }
  throw new Error(`nats-server did not become ready on ${url}; see ${join(home, "nats.log")}`);
}

/** Joins argv paths the way the platform writes them. File-system calls keep the host join. */
const joinFor = (platform: NodeJS.Platform) => (platform === "win32" ? win32.join : join);

/** Runs a Windows probe that may be missing or fail; a failed probe reads as "no answer". */
function probe(d: Resolved, cmd: string[]): string {
  try {
    return d.run(cmd);
  } catch {
    return "";
  }
}

const argsOf = (pid: number, d: Resolved): string =>
  d.platform === "win32"
    ? probe(d, ["powershell", "-NoProfile", "-Command", `(Get-CimInstance Win32_Process -Filter 'ProcessId=${pid}').CommandLine`]).trim()
    : d.run(["ps", "-p", String(pid), "-o", "args="]).trim();

/** Whether a command line is a nats-server whose store dir is exactly this home's. */
function servesHome(args: string, home: string, platform: NodeJS.Platform): boolean {
  if (platform !== "win32") {
    const t = args.split(/\s+/);
    return /(^|\/)nats-server$/.test(t[0] ?? "") && t.some((x, i) => x === "-sd" && t[i + 1] === join(home, "jetstream"));
  }
  // Windows quotes paths with spaces; the store dir matches case-insensitively and with either separator.
  const t = [...args.matchAll(/"([^"]*)"|(\S+)/g)].map((m) => m[1] ?? m[2] ?? "");
  const norm = (s: string) => s.replace(/\//g, "\\").toLowerCase();
  const dir = norm(win32.join(home, "jetstream"));
  return /(^|[\\/])nats-server(\.exe)?$/i.test(t[0] ?? "") && t.some((x, i) => x === "-sd" && norm(t[i + 1] ?? "") === dir);
}

export type StopResult = "stopped" | "no-pid-file" | "not-ours" | "stale";

/** Pids of live nats-servers whose store dir is this home's. */
function pidsServing(home: string, d: Resolved): number[] {
  if (d.platform === "win32") {
    const listing = probe(d, [
      "powershell",
      "-NoProfile",
      "-Command",
      `Get-CimInstance Win32_Process -Filter "Name='nats-server.exe'" | ForEach-Object { "$($_.ProcessId) $($_.CommandLine)" }`,
    ]);
    return listing
      .split(/\r?\n/)
      .map((l) => /^(\d+) (.*)$/.exec(l))
      .filter((m): m is RegExpExecArray => m !== null && servesHome(m[2] ?? "", home, "win32"))
      .map((m) => Number(m[1]));
  }
  const others = d.run(["pgrep", "-f", "--", `-sd ${join(home, "jetstream")}`]).split(/\s+/).filter(Boolean).map(Number);
  return others.filter((p) => servesHome(argsOf(p, d), home, d.platform));
}

/** Stops this home's detached server. A stale pid file is removed; a pid naming any other process is left alone. */
export function stopServer({ home }: Pick<Where, "home">, deps?: Deps): StopResult {
  const d = withDefaults(deps);
  const pidFile = join(home, "nats.pid");
  let pid: number;
  try {
    pid = Number(readFileSync(pidFile, "utf8").trim());
  } catch {
    return "no-pid-file";
  }
  const kill = (p: number) => {
    try {
      d.kill(p, "SIGTERM");
      return true;
    } catch {
      return false;
    }
  };
  const args = Number.isInteger(pid) && pid > 0 ? argsOf(pid, d) : "";
  if (args) {
    if (!servesHome(args, home, d.platform) || !kill(pid)) return "not-ours";
    rmSync(pidFile, { force: true });
    return "stopped";
  }
  // The recorded pid is dead. Our server may still run under another pid (pid file rewritten or lost).
  rmSync(pidFile, { force: true });
  const ours = pidsServing(home, d);
  return ours.length && ours.every(kill) ? "stopped" : "stale";
}

export const serverStatus = ({ url }: Pick<Where, "url">) => jetstreamReady(url);
