import { spawn, type Subprocess } from "bun";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { MemoryBus } from "../src/bus";
import { readIdentity } from "../src/identity";
import { findServerBin, stopServer } from "../src/server";

const ROOT = join(import.meta.dir, "..");

function freePort(): number {
  const s = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } });
  const p = s.port;
  s.stop(true);
  return p;
}

/** A real nats-server child on a temp home and random localhost port. Throws (never skips) when the binary is missing. */
export class NatsHarness {
  readonly home = mkdtempSync(join(process.env.AMB_TEST_TMP ?? tmpdir(), "amb-test-"));
  readonly port = freePort();
  readonly url = `nats://127.0.0.1:${this.port}`;
  private proc?: Subprocess;
  private readonly bin = findServerBin();

  async start() {
    this.proc = spawn([this.bin, "-js", "-sd", join(this.home, "js"), "-a", "127.0.0.1", "-p", String(this.port)], {
      stdout: "ignore", stderr: "ignore",
    });
    for (let i = 0; i < 100; i++) {
      try {
        const c = await Bun.connect({ hostname: "127.0.0.1", port: this.port, socket: { data() {} } });
        c.end();
        return;
      } catch {
        await Bun.sleep(50);
      }
    }
    throw new Error("nats-server did not start");
  }

  get serverPid() {
    return this.proc?.pid;
  }

  async stop(signal: "SIGTERM" | "SIGKILL" = "SIGTERM") {
    if (!this.proc) return;
    this.proc.kill(signal);
    await this.proc.exited;
    this.proc = undefined;
  }

  async restart(signal: "SIGTERM" | "SIGKILL") {
    await this.stop(signal);
    await this.start();
  }

  async dispose() {
    await this.stop();
    stopServer({ home: this.home }); // in case an amb mcp child started a detached server here

    rmSync(this.home, { recursive: true, force: true });
  }

  env(extra: Record<string, string>): Record<string, string> {
    return { AMB_URL: this.url, AMB_HOME: this.home, AMB_PROJECT: "amb", AMB_ROLE: "agent", ...extra };
  }

  async open(extra: Record<string, string>) {
    return MemoryBus.open(readIdentity(this.env(extra)));
  }

  async mcp(extra: Record<string, string>): Promise<McpAgent> {
    const transport = new StdioClientTransport({
      command: process.execPath, args: [join(ROOT, "src/main.ts"), "mcp"], env: this.env(extra), stderr: "inherit",
    });
    const client = new Client({ name: "harness", version: "0" });
    await client.connect(transport);
    return new McpAgent(client, transport);
  }
}

export class McpAgent {
  constructor(readonly client: Client, readonly transport: StdioClientTransport) {}

  async call(name: string, args: Record<string, unknown> = {}) {
    const r = await this.client.callTool({ name, arguments: args }, undefined, { timeout: 60_000 });
    const text = (r.content as { type: string; text: string }[])[0]?.text ?? "";
    return { isError: Boolean(r.isError), text, data: r.structuredContent as any, items: parseEnvelope(text) as any[] };
  }

  get pid() {
    return this.transport.pid!;
  }

  close() {
    return this.client.close();
  }
}

/** The JSON lines inside the untrusted envelope, as a model would read them from the text. */
export function parseEnvelope(text: string): unknown[] {
  const lines = text.split("\n");
  const open = lines.findIndex((l) => l.startsWith("<untrusted_events"));
  const close = lines.lastIndexOf("</untrusted_events>");
  return open < 0 || close < 0 ? [] : lines.slice(open + 1, close).map((l) => JSON.parse(l));
}

/** Polls `f` until it returns a truthy value or the deadline passes. */
export async function until<T>(f: () => Promise<T | undefined | false>, ms = 10_000, every = 50): Promise<T> {
  for (const deadline = Date.now() + ms; ; await Bun.sleep(every)) {
    const v = await f();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`condition not met within ${ms} ms`);
  }
}
