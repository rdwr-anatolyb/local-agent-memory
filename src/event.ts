import { z } from "zod";

export const EVENT_TYPES = [
  "task_assigned", "task_started", "heartbeat", "task_result",
  "finding", "decision", "code_change", "error", "question", "help_request", "handoff",
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

const STRICT_DETAILS: Partial<Record<EventType, z.ZodType>> = {
  task_assigned: AssignedDetails,
  task_result: ResultDetails,
  heartbeat: HeartbeatDetails,
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
