import { homedir } from "node:os";
import { join } from "node:path";
import { isToken } from "./event";

export type Role = "worker" | "planner" | "dashboard" | "agent" | "orchestrator";
const ROLES: Role[] = ["worker", "planner", "dashboard", "agent", "orchestrator"];

export type Thresholds = { stallAfterMs: number; claimTimeoutMs: number; quietAfterMs: number };

export type Identity = {
  agentId: string;
  projectId: string;
  role: Role;
  story?: string;
  task?: string;
  filter: string[];
  cursorStart: "all" | "new";
  url: string;
  home: string;
  heartbeatMs: number;
  ackWaitMs: number;
  /** How far back get_status looks for active stories when no story is named. */
  statusWindowMs: number;
  thresholds: Thresholds;
};

type Env = Record<string, string | undefined>;

export function readIdentity(env: Env): Identity {
  const get = (k: string) => {
    const v = env[k];
    if (v === undefined || v === "") return undefined;
    // Claude Code keeps a literal ${VAR} when the variable is unset
    if (v.startsWith("${")) throw new Error(`${k} is an unexpanded placeholder (${v}); set it in the spawning environment`);
    return v;
  };
  const token = (k: string, required: boolean) => {
    const v = get(k);
    if (v === undefined) {
      if (required) throw new Error(`${k} is required`);
      return undefined;
    }
    if (!isToken(v)) throw new Error(`${k} must match ^[A-Za-z0-9_-]{1,64}$, got ${JSON.stringify(v)}`);
    return v;
  };
  const num = (k: string, d: number) => {
    const v = get(k);
    if (v === undefined) return d;
    const n = Number(v);
    // setTimeout and JetStream nanos need whole milliseconds that fit a signed 32-bit int.
    if (!Number.isInteger(n) || n < 1 || n > 2 ** 31 - 1) throw new Error(`${k} must be a whole number of ms from 1 to ${2 ** 31 - 1}`);
    return n;
  };

  const agentId = token("AMB_AGENT_ID", true)!;
  const projectId = token("AMB_PROJECT", true)!;
  // Fail closed: a missing role must not silently become a broad ad hoc identity.
  const role = get("AMB_ROLE") as Role | undefined;
  if (role === undefined) throw new Error(`AMB_ROLE is required (one of ${ROLES.join(", ")})`);
  if (!ROLES.includes(role)) throw new Error(`AMB_ROLE must be one of ${ROLES.join(", ")}`);
  const story = token("AMB_STORY", role === "worker");
  const task = token("AMB_TASK", role === "worker");
  for (const [k, v] of [["AMB_STORY", story], ["AMB_TASK", task]]) if (v === "_") throw new Error(`${k} cannot be "_" (reserved for none)`);
  // A stamped default story would quietly file events for other stories under this one.
  if (role === "orchestrator" && (story || task)) throw new Error("orchestrator acts on every story in its project; unset AMB_STORY and AMB_TASK");

  const P = `amb.ev.${projectId}`;
  let filter: string[];
  let cursorStart: "all" | "new" = "all";
  if (role === "worker") filter = [`${P}.${story}.${task}.>`, `${P}.${story}._.>`];
  else if (role === "planner") filter = [story ? `${P}.${story}.>` : `${P}.>`];
  else if (role === "dashboard") [filter, cursorStart] = [[`${P}.>`], "new"];
  else if (role === "orchestrator") filter = [`${P}.>`];
  else {
    filter = get("AMB_FILTER")?.split(",").map((s) => s.trim()) ?? [`${P}.>`];
    // Stage 1 has no auth; at least keep an ad hoc cursor inside its own project.
    for (const f of filter)
      if (!f.startsWith(`${P}.`) || /\s/.test(f)) throw new Error(`AMB_FILTER entries must start with ${P}. (got ${JSON.stringify(f)})`);
    const cs = get("AMB_CURSOR_START");
    if (cs !== undefined && cs !== "all" && cs !== "new") throw new Error("AMB_CURSOR_START must be all or new");
    cursorStart = cs ?? "all";
  }

  return {
    agentId, projectId, role, story, task, filter, cursorStart,
    url: get("AMB_URL") ?? "nats://127.0.0.1:4777",
    home: get("AMB_HOME") ?? join(homedir(), ".agent-memory-bus"),
    heartbeatMs: num("AMB_HEARTBEAT_MS", 30_000),
    ackWaitMs: num("AMB_ACK_WAIT_MS", 30_000),
    statusWindowMs: num("AMB_STATUS_WINDOW_MS", 24 * 3600_000),
    thresholds: {
      stallAfterMs: num("AMB_STALL_AFTER_MS", 90_000),
      claimTimeoutMs: num("AMB_CLAIM_TIMEOUT_MS", 10 * 60_000),
      quietAfterMs: num("AMB_QUIET_AFTER_MS", 30 * 60_000),
    },
  };
}
