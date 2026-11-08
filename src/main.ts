#!/usr/bin/env bun
import { homedir } from "node:os";
import { join } from "node:path";
import { runMcp } from "./mcp";
import { ensureServer, serverStatus, stopServer } from "./server";

const where = {
  url: process.env.AMB_URL || "nats://127.0.0.1:4777",
  home: process.env.AMB_HOME || join(homedir(), ".agent-memory-bus"),
};
const [cmd, sub] = process.argv.slice(2);

try {
  if (cmd === "mcp") await runMcp();
  else if (cmd === "server" && sub === "start") console.log(await ensureServer(where), where.url);
  else if (cmd === "server" && sub === "stop") {
    const r = stopServer(where);
    console.log({
      "stopped": "stopped",
      "no-pid-file": `not running (no pid file in ${where.home})`,
      "not-ours": "the pid file names a process that is not this home's nats-server; left it alone",
      "stale": "not running (the pid file named a dead process; removed it)",
    }[r]);
  }
  else if (cmd === "server" && sub === "status") console.log((await serverStatus(where)) ? "running" : "down", where.url);
  else {
    console.error("usage: amb mcp | amb server start|stop|status");
    process.exit(2);
  }
} catch (e) {
  console.error(`amb: ${e instanceof Error ? e.message : e}`);
  process.exit(1);
}
