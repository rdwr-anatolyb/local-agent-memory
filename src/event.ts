import { createHash } from "node:crypto";
import { z } from "zod";

export const EVENT_TYPES = [
  "task_assigned", "task_started", "heartbeat", "task_result",
  "finding", "decision", "code_change", "error", "question", "help_request", "handoff",
  "kill", "gate_pending", "gate_decided", "status_changed", "child_link",
] as const;
export type EventType = (typeof EVENT_TYPES)[number];

const TOKEN_RE = /^[A-Za-z0-9_-]{1,64}$/;
export const isToken = (s: string) => TOKEN_RE.test(s);
export const Token = z.string().regex(TOKEN_RE, "must match ^[A-Za-z0-9_-]{1,64}$");

/** A story or task id: a token other than "_", which subjects use for "none". */
const Id = Token.refine((s) => s !== "_", "\"_\" is reserved for none");

export const MAX_SUMMARY = 500;
export const MAX_EVENT_BYTES = 64 * 1024;

const Attempt = z.number().int().positive();
/** Events a worker publishes about its own task; only the assignee's count in status. */
export const LIFECYCLE: ReadonlySet<EventType> = new Set(["task_started", "task_result", "error"]);

// A pointer, never prose: AmbEvent below requires it to equal sdlc-flow://<this story>/<this task> exactly.
const BriefRef = z.string().max(200);
export const AssignedDetails = z.strictObject({ brief_ref: BriefRef, attempt: Attempt });
export const ResultDetails = z.strictObject({
  outcome: z.enum(["completed", "failed"]),
  attempt: Attempt,
  artifacts: z.array(z.strictObject({ kind: z.enum(["branch", "commit", "pr", "file"]), ref: z.string().min(1).max(500) })).optional(),
});
export const HeartbeatDetails = z.strictObject({ attempt: Attempt, seq_of_task_started: z.number().int().nonnegative() });

/** Orchestrator-only events that describe a flow (spec section 4), not an agent's work. */
export const FLOW_EVENTS: ReadonlySet<EventType> = new Set(["kill", "gate_pending", "gate_decided", "status_changed", "child_link"]);
const Gate = { gate_id: z.string().min(1).max(200) }; // sdlc-flow's raw `<node key>#<iteration>.<attempt>`, payload only
const KillDetails = z.strictObject({ by_parent: Id.optional() }).optional();
const GatePendingDetails = z.strictObject({ ...Gate, doc_ref: z.string().min(1).max(500).optional() });
const GateDecidedDetails = z.strictObject({
  ...Gate,
  decision: z.enum(["approve", "reject", "retry", "drop", "cancel"]),
  feedback_ref: z.string().min(1).max(500).optional(),
});
const StatusChangedDetails = z.strictObject({ from: z.string().min(1).max(32).nullable(), to: z.string().min(1).max(32) });
const ChildLinkDetails = z.strictObject({ parent: Id });

const STRICT_DETAILS: Partial<Record<EventType, z.ZodType>> = {
  task_assigned: AssignedDetails,
  task_result: ResultDetails,
  heartbeat: HeartbeatDetails,
  kill: KillDetails,
  gate_pending: GatePendingDetails,
  gate_decided: GateDecidedDetails,
  status_changed: StatusChangedDetails,
  child_link: ChildLinkDetails,
};

export const AmbEvent = z
  .object({
    event_id: z.string().min(1).max(64),
    timestamp: z.iso.datetime(),
    session_id: z.string().min(1).max(64),
    project_id: Token,
    correlation_id: Id.nullable(),
    task_id: Id.nullable(),
    agent_id: Token,
    event_type: z.enum(EVENT_TYPES),
    tags: z.array(z.string().min(1).max(64)).max(32),
    to_agent: Token.nullable(),
    to_task: Token.nullable(),
    payload: z.object({ summary: z.string().max(MAX_SUMMARY), details: z.unknown().optional() }),
  })
  .superRefine((ev, ctx) => {
    if (ev.event_type === "task_assigned") {
      if (!ev.to_agent) ctx.addIssue({ code: "custom", path: ["to_agent"], message: "task_assigned needs to_agent" });
      if (!ev.correlation_id || !ev.task_id) ctx.addIssue({ code: "custom", path: ["task_id"], message: "task_assigned needs a story and a task" });
      const ref = (ev.payload.details as { brief_ref?: unknown } | undefined)?.brief_ref;
      if (typeof ref === "string" && ref !== `sdlc-flow://${ev.correlation_id}/${ev.task_id}`)
        ctx.addIssue({ code: "custom", path: ["payload", "details", "brief_ref"], message: "must be sdlc-flow://<this story>/<this task>" });
    }
    if (FLOW_EVENTS.has(ev.event_type) && !ev.correlation_id)
      ctx.addIssue({ code: "custom", path: ["correlation_id"], message: `${ev.event_type} needs a story` });
    if ((ev.event_type === "gate_pending" || ev.event_type === "gate_decided") && !ev.task_id)
      ctx.addIssue({ code: "custom", path: ["task_id"], message: `${ev.event_type} needs a node (task_id)` });
    const schema = STRICT_DETAILS[ev.event_type];
    if (!schema) return;
    const r = schema.safeParse(ev.payload.details);
    if (!r.success) for (const i of r.error.issues) ctx.addIssue({ ...i, path: ["payload", "details", ...i.path] } as never);
  });
export type AmbEventT = z.infer<typeof AmbEvent>;
export type StoredEvent = AmbEventT & { seq: number };

export const subjectFor = (e: Pick<AmbEventT, "project_id" | "correlation_id" | "task_id" | "agent_id" | "event_type">) =>
  `amb.ev.${e.project_id}.${e.correlation_id ?? "_"}.${e.task_id ?? "_"}.${e.agent_id}.${e.event_type}`;

export const hbSubject = (project: string, story: string, task: string, agent: string) =>
  `amb.hb.${project}.${story}.${task}.${agent}`;

export type SubjectQuery = {
  project_id: string; correlation_id?: string; task_id?: string; agent_id?: string; event_type?: EventType;
};

/** One server-side filter; every event subject has exactly five tokens after `amb.ev`. */
export function filterFor(q: SubjectQuery): string {
  const parts = [q.project_id, q.correlation_id, q.task_id, q.agent_id, q.event_type];
  for (const p of parts) if (p !== undefined && !isToken(p)) throw new Error(`invalid subject token: ${JSON.stringify(p)}`);
  return `amb.ev.${parts.map((p) => p ?? "*").join(".")}`;
}

const SIMPLE_ID = /^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*$/;
/**
 * sdlc-flow Task IDs and node keys as bus tokens. Simple ids swap `.` for `_` and decode back; any other id gets a
 * readable prefix and a sha256 suffix. Simple outputs never contain `__`, so the two forms cannot collide.
 */
export function encodeToken(raw: string): string {
  if (raw === "") throw new Error("encodeToken: empty id");
  if (SIMPLE_ID.test(raw) && raw.length <= 64) return raw.replaceAll(".", "_");
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 12);
  return `${raw.replaceAll(".", "_").replace(/[^A-Za-z0-9_-]/g, "-").slice(0, 50)}__${hash}`;
}

/** The raw id of a simple token; undefined for a hashed one, which callers map back by re-encoding known ids. */
export const decodeToken = (t: string): string | undefined => (t.includes("__") ? undefined : t.replaceAll("_", "."));
