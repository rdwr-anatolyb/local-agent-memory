import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "..");

describe("library entry", () => {
  test.each(["src/index.ts", "src/main.ts"])("importing %s starts nothing and prints nothing", (file) => {
    const home = mkdtempSync(join(tmpdir(), "amb-lib-"));
    try {
      const r = Bun.spawnSync([process.execPath, "-e", `await import(${JSON.stringify(join(ROOT, file))})`], {
        env: { ...process.env, AMB_HOME: home, AMB_URL: "nats://127.0.0.1:1" },
        stdout: "pipe", stderr: "pipe",
      });
      expect(r.stdout.toString()).toBe("");
      expect(r.stderr.toString()).toBe("");
      expect(r.exitCode).toBe(0);
      expect(existsSync(join(home, "nats.pid"))).toBe(false);
    } finally {
      rmSync(home, { recursive: true, force: true });
    }
  });

  test("the exports map names the library entry with its API", async () => {
    const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8")) as { exports: Record<string, string> };
    const lib = await import(join(ROOT, pkg.exports["."]!));
    expect(typeof lib.MemoryBus.open).toBe("function");
    expect(typeof lib.readIdentity).toBe("function");
    expect(typeof lib.ensureServer).toBe("function");
    expect(typeof lib.stopServer).toBe("function");
    expect(typeof lib.serverStatus).toBe("function");
    expect(typeof lib.AmbEvent.safeParse).toBe("function");
    expect(typeof lib.encodeToken).toBe("function");
    expect(typeof lib.decodeToken).toBe("function");
    expect(lib.mcp).toBeUndefined();
  });
});
