import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, win32 } from "node:path";
import type { spawn } from "node:child_process";
import { ensureServer, findServerBin, stopServer } from "../src/server";

type SpawnCall = { bin: string; args: string[]; opts: Record<string, unknown> };

/** Fake spawn: records the call, reports the server ready right after, and counts unref. */
function fakeSpawn() {
  const calls: SpawnCall[] = [];
  const state = { unref: 0, ready: false };
  const spawnFn = ((bin: string, args: string[], opts: Record<string, unknown>) => {
    calls.push({ bin, args, opts });
    state.ready = true;
    return { unref: () => void state.unref++ };
  }) as unknown as typeof spawn;
  return { calls, state, spawnFn };
}

describe("findServerBin with injected platform", () => {
  test("win32 falls back to nats-server.exe when the bare name is not found", () => {
    const which = (n: string) => (n === "nats-server.exe" ? "C:\\nats\\nats-server.exe" : null);
    expect(findServerBin({}, { platform: "win32", which })).toBe("C:\\nats\\nats-server.exe");
  });

  test("darwin never tries the .exe name and its error names brew", () => {
    const seen: string[] = [];
    const which = (n: string) => {
      seen.push(n);
      return null;
    };
    expect(() => findServerBin({}, { platform: "darwin", which })).toThrow("brew install nats-server");
    expect(seen).toEqual(["nats-server"]);
  });

  test("win32 error names AMB_NATS_SERVER_BIN and does not mention brew", () => {
    let msg = "";
    try {
      findServerBin({}, { platform: "win32", which: () => null });
    } catch (e) {
      msg = String(e);
    }
    expect(msg).toContain("AMB_NATS_SERVER_BIN");
    expect(msg).not.toContain("brew");
  });

  test("AMB_NATS_SERVER_BIN wins over which on both platforms", () => {
    const which = () => "C:\\other\\nats-server.exe";
    expect(findServerBin({ AMB_NATS_SERVER_BIN: "D:\\x\\nats-server.exe" }, { platform: "win32", which })).toBe("D:\\x\\nats-server.exe");
    expect(findServerBin({ AMB_NATS_SERVER_BIN: "/opt/nats" }, { platform: "darwin", which })).toBe("/opt/nats");
  });

  test("an empty AMB_NATS_SERVER_BIN is ignored", () => {
    expect(findServerBin({ AMB_NATS_SERVER_BIN: "" }, { platform: "darwin", which: () => "/usr/local/bin/nats-server" })).toBe(
      "/usr/local/bin/nats-server",
    );
  });
});

describe("stopServer on win32 (fake run and kill)", () => {
  const exe = `"C:\\Program Files\\nats\\nats-server.exe"`;
  const home = () => mkdtempSync(join(tmpdir(), "amb-win-"));
  const dirOf = (h: string) => win32.join(h, "jetstream");

  /** Writes a pid file, answers every probe with `output`, and returns the result plus the pids killed. */
  function stopWith(h: string, output: string, pid = 4242) {
    writeFileSync(join(h, "nats.pid"), String(pid));
    const killed: number[] = [];
    const r = stopServer({ home: h }, { platform: "win32", run: () => output, kill: (p) => void killed.push(p) });
    return { r, killed };
  }

  test("a quoted exe path with spaces and a mixed-case store dir is ours: stopped, pid killed, pid file removed", () => {
    const h = home();
    try {
      const { r, killed } = stopWith(h, `${exe} -sd "${dirOf(h).replace(/jetstream$/, "JetStream")}"`);
      expect(r).toBe("stopped");
      expect(killed).toEqual([4242]);
      expect(existsSync(join(h, "nats.pid"))).toBe(false);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("a store dir written with forward slashes is still ours", () => {
    const h = home();
    try {
      const { r } = stopWith(h, `${exe} -sd "${dirOf(h).replace(/\\/g, "/")}"`);
      expect(r).toBe("stopped");
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("another home's nats-server is not-ours and is not killed", () => {
    const h = home();
    try {
      const { r, killed } = stopWith(h, `${exe} -sd "C:\\other\\jetstream"`);
      expect(r).toBe("not-ours");
      expect(killed).toEqual([]);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("notepad.exe naming this home's store dir is not-ours", () => {
    const h = home();
    try {
      const { r, killed } = stopWith(h, `"C:\\Windows\\notepad.exe" -sd "${dirOf(h)}"`);
      expect(r).toBe("not-ours");
      expect(killed).toEqual([]);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("nats-server.exe.bak naming this home's store dir is not-ours", () => {
    const h = home();
    try {
      const { r, killed } = stopWith(h, `"C:\\nats\\nats-server.exe.bak" -sd "${dirOf(h)}"`);
      expect(r).toBe("not-ours");
      expect(killed).toEqual([]);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("empty probe output with a dead pid is stale and the pid file is removed", () => {
    const h = home();
    try {
      const { r, killed } = stopWith(h, "");
      expect(r).toBe("stale");
      expect(killed).toEqual([]);
      expect(existsSync(join(h, "nats.pid"))).toBe(false);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("a probe that throws is stale, does not crash, and removes the pid file", () => {
    const h = home();
    try {
      writeFileSync(join(h, "nats.pid"), "4242");
      const r = stopServer(
        { home: h },
        {
          platform: "win32",
          run: () => {
            throw new Error("powershell not found");
          },
          kill: () => {
            throw new Error("should not kill");
          },
        },
      );
      expect(r).toBe("stale");
      expect(existsSync(join(h, "nats.pid"))).toBe(false);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });
});

describe("ensureServer spawn shape", () => {
  const url = "nats://127.0.0.1:4777";

  test("win32: windowsHide, detached, unref once, and backslash -sd and -P paths", async () => {
    const h = mkdtempSync(join(tmpdir(), "amb-win-"));
    try {
      const f = fakeSpawn();
      const r = await ensureServer({ url, home: h }, {
        platform: "win32",
        which: () => "C:\\nats\\nats-server.exe",
        ready: async () => f.state.ready,
        spawn: f.spawnFn,
      });
      expect(r).toBe("started");
      expect(f.calls.length).toBe(1);
      expect(f.calls[0]!.bin).toBe("C:\\nats\\nats-server.exe");
      expect(f.calls[0]!.opts).toMatchObject({ detached: true, windowsHide: true });
      expect(f.calls[0]!.args).toEqual(
        expect.arrayContaining(["-sd", win32.join(h, "jetstream"), "-P", win32.join(h, "nats.pid")]),
      );
      expect(f.state.unref).toBe(1);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });

  test("darwin: argv and spawn options are the same as before (no windowsHide)", async () => {
    const h = mkdtempSync(join(tmpdir(), "amb-darwin-"));
    try {
      const f = fakeSpawn();
      await ensureServer({ url, home: h }, {
        platform: "darwin",
        which: () => "/usr/local/bin/nats-server",
        ready: async () => f.state.ready,
        spawn: f.spawnFn,
      });
      expect(f.calls[0]!.bin).toBe("/usr/local/bin/nats-server");
      expect(f.calls[0]!.args).toEqual([
        "-js", "-sd", join(h, "jetstream"), "-a", "127.0.0.1", "-p", "4777", "-P", join(h, "nats.pid"), "-n", "amb",
      ]);
      expect(f.calls[0]!.opts).toEqual({ detached: true, stdio: ["ignore", expect.any(Number), expect.any(Number)] });
      expect(f.state.unref).toBe(1);
    } finally {
      rmSync(h, { recursive: true, force: true });
    }
  });
});
