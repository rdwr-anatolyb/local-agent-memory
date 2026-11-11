import { describe, expect, test } from "bun:test";
import type { EventType, StoredEvent } from "../src/event";
import { deriveStatus, type Heartbeat } from "../src/status";

const T0 = Date.parse("2026-11-07T10:00:00.000Z");
const th = { stallAfterMs: 90_000, claimTimeoutMs: 600_000, quietAfterMs: 1_800_000 };
let seq = 0;
function ev(event_type: EventType, agent_id: string, atMs: number, o: Partial<StoredEvent> & { details?: unknown } = {}): StoredEvent {
  const { details, ...rest } = o;
  return {
    event_id: `e${++seq}`, seq, timestamp: new Date(T0 + atMs).toISOString(), session_id: "s", project_id: "P",
    correlation_id: "S", task_id: "T1", agent_id, event_type, tags: [], to_agent: null, to_task: null,
    payload: { summary: event_type, details }, ...rest,
  };
}
const assign = (attempt: number, at = 0, task = "T1") =>
  ev("task_assigned", "planner", at, { task_id: task, to_agent: `w-${task}-${attempt}`, details: { brief_ref: `r/${task}`, attempt } });
const hb = (agent: string, atMs: number): Heartbeat => ({ agent_id: agent, timestamp: new Date(T0 + atMs).toISOString() });
const one = (events: StoredEvent[], hbs: Heartbeat[], nowMs: number) => deriveStatus(events, hbs, T0 + nowMs, th)[0]!.tasks[0]!;

describe("deriveStatus states (first match wins)", () => {
  test("assigned", () => expect(one([assign(1)], [], 1000).state).toBe("assigned"));
  test("unclaimed after claim_timeout, not at the edge", () => {
    expect(one([assign(1)], [], 600_000).state).toBe("assigned");
    expect(one([assign(1)], [], 600_001).state).toBe("unclaimed");
  });
  const started = [assign(1), ev("task_started", "w-T1-1", 1000)];
  test("started with fresh heartbeat", () => expect(one(started, [hb("w-T1-1", 50_000)], 60_000).state).toBe("started"));
  test("stalled when heartbeat is older than stall_after", () =>
    expect(one(started, [hb("w-T1-1", 10_000)], 100_001).state).toBe("stalled"));
  test("stalled with no heartbeat only after stall_after from start", () => {
    expect(one(started, [], 50_000).state).toBe("started");
    expect(one(started, [], 91_001).state).toBe("stalled");
  });
  test("quiet: fresh heartbeat but no event for quiet_after", () =>
    expect(one(started, [hb("w-T1-1", 1_850_000)], 1_860_000).state).toBe("quiet"));
  test("completed via task_result", () => {
    const r = ev("task_result", "w-T1-1", 2000, { details: { outcome: "completed", attempt: 1 } });
    const s = one([...started, r], [], 999_999_999);
    expect(s.state).toBe("completed");
    expect(s.result?.seq).toBe(r.seq);
  });
  test("failed via outcome failed or error", () => {
    expect(one([...started, ev("task_result", "w-T1-1", 2000, { details: { outcome: "failed", attempt: 1 } })], [], 5000).state).toBe("failed");
    expect(one([...started, ev("error", "w-T1-1", 2000)], [], 5000).state).toBe("failed");
  });
  test("a finding resets quiet", () =>
    expect(one([...started, ev("finding", "w-T1-1", 1_850_000)], [hb("w-T1-1", 1_855_000)], 1_860_000).state).toBe("started"));
});

describe("attempts", () => {
  test("latest attempt wins; old attempt is superseded; a late result from it changes nothing", () => {
    const events = [
      assign(1), ev("task_started", "w-T1-1", 1000),
      assign(2, 200_000), ev("task_started", "w-T1-2", 201_000),
      ev("task_result", "w-T1-2", 300_000, { details: { outcome: "completed", attempt: 2 } }),
      ev("task_result", "w-T1-1", 400_000, { details: { outcome: "failed", attempt: 1 } }),
    ];
    const t = one(events, [], 500_000);
    expect(t).toMatchObject({ attempt: 2, assignee: "w-T1-2", state: "completed" });
    expect(t.result?.agent_id).toBe("w-T1-2");
    expect(t.superseded).toEqual([{ attempt: 1, agent: "w-T1-1", state: "failed" }]);
  });
  test("unassigned ad hoc agent still shows as a task", () => {
    const t = one([ev("task_started", "solo", 0)], [hb("solo", 1000)], 2000);
    expect(t).toMatchObject({ assignee: "solo", state: "started", attempt: 0 });
  });
  test("H1: terminal events count only from the attempt's assignee", () => {
    const forged = [assign(1), ev("task_result", "w-T2-1", 10, { details: { outcome: "completed", attempt: 99 } }), ev("error", "planner", 20)];
    expect(one(forged, [], 100)).toMatchObject({ state: "assigned", attempt: 1, assignee: "w-T1-1", superseded: [] });
  });
  test("H1: the attempt number comes only from task_assigned", () => {
    const evs = [assign(1), ev("task_started", "w-T1-1", 10, { details: { attempt: 7 } }), ev("task_result", "w-T1-1", 20, { details: { outcome: "completed", attempt: 5 } })];
    expect(one(evs, [], 100)).toMatchObject({ state: "completed", attempt: 1 });
  });
  test("H1: a non-assignee's task_started on an assigned task is ignored", () =>
    expect(one([assign(1), ev("task_started", "sdlc-flow", 10)], [], 100)).toMatchObject({ state: "assigned", assignee: "w-T1-1", superseded: [] }));
  test("N5: a re-sent task_assigned for the same attempt does not reset it", () => {
    const evs = [assign(1), ev("task_started", "w-T1-1", 10), ev("task_result", "w-T1-1", 20, { details: { outcome: "completed", attempt: 1 } }), assign(1, 30)];
    expect(one(evs, [], 999_999_999)).toMatchObject({ state: "completed", attempt: 1, superseded: [] });
  });
  test("N5: the same agent with a new attempt number starts a new attempt", () => {
    const evs = [assign(1), ev("task_started", "w-T1-1", 10), ev("error", "w-T1-1", 20), { ...assign(2, 30), to_agent: "w-T1-1" }];
    expect(one(evs, [], 100)).toMatchObject({ state: "assigned", attempt: 2 });
  });
  test("H4: a non-lifecycle event never creates a task or an attempt", () => {
    const stories = deriveStatus([assign(1, 0, "T1"), ev("question", "w-T1-1", 10, { task_id: "T2" }), ev("finding", "planner", 20)], [], T0 + 100, th);
    expect(stories[0]!.tasks.map((t) => [t.task_id, t.attempt, t.superseded])).toEqual([["T1", 1, []]]);
  });
  test("T2: a late completed result from attempt 1 does not finish a started attempt 2", () => {
    const evs = [
      assign(1), ev("task_started", "w-T1-1", 1000), assign(2, 200_000), ev("task_started", "w-T1-2", 201_000),
      ev("task_result", "w-T1-1", 250_000, { details: { outcome: "completed", attempt: 1 } }),
    ];
    const t = one(evs, [hb("w-T1-2", 255_000)], 260_000);
    expect(t).toMatchObject({ state: "started", attempt: 2, assignee: "w-T1-2", result: undefined });
    expect(t.superseded).toEqual([{ attempt: 1, agent: "w-T1-1", state: "completed" }]);
  });
});

describe("stories", () => {
  test("groups by story and task; done only when every task completed; lastSeq", () => {
    const a = assign(1, 0, "T1"), b = assign(1, 0, "T2");
    const r1 = ev("task_result", "w-T1-1", 10, { task_id: "T1", details: { outcome: "completed", attempt: 1 } });
    const other = ev("finding", "x", 10, { correlation_id: "S2", task_id: "T9" });
    const stories = deriveStatus([a, b, r1, other], [], T0 + 20, th);
    const s = stories.find((x) => x.correlation_id === "S")!;
    expect(s.tasks.map((t) => [t.task_id, t.state])).toEqual([["T1", "completed"], ["T2", "assigned"]]);
    expect(s.done).toBe(false);
    expect(s.lastSeq).toBe(r1.seq);
    const r2 = ev("task_result", "w-T2-1", 10, { task_id: "T2", details: { outcome: "completed", attempt: 1 } });
    expect(deriveStatus([a, b, r1, r2], [], T0 + 20, th)[0]!.done).toBe(true);
  });
  test("N10: stories without an assigned or started task are not listed", () =>
    expect(deriveStatus([ev("decision", "planner", 0, { task_id: null }), ev("finding", "x", 0, { correlation_id: "S9" })], [], T0, th)).toEqual([]));
});

// --- get_status lines (src/mcp.ts statusLine) and heartbeat parsing (src/bus.ts heartbeatFrom) ---
import { heartbeatFrom } from "../src/bus";
import { statusLine } from "../src/mcp";
import { lineBytes, LINES_BUDGET } from "../src/untrusted";
import type { StoryStatus, TaskStatus } from "../src/status";

function storyOf(n: number, o: { summary?: string; artifacts?: number; ref?: string; superseded?: number; assignee?: string; results?: boolean } = {}): StoryStatus {
  const tasks: TaskStatus[] = Array.from({ length: n }, (_, i) => ({
    correlation_id: "S", task_id: `T${i}`, attempt: 1, assignee: (o.assignee ?? "w-") + i, state: o.results === false ? "assigned" : "completed",
    lastEventSeq: 100 + i, lastHeartbeat: "2026-11-07T10:00:00.000Z",
    result: o.results === false ? undefined : {
      ...ev("task_result", `w-${i}`, 0), seq: 200 + i,
      payload: { summary: o.summary ?? "done", details: { outcome: "completed", attempt: 1, artifacts: Array.from({ length: o.artifacts ?? 0 }, (_, k) => ({ kind: "file", ref: (o.ref ?? "r") + k })) } },
    },
    superseded: Array.from({ length: o.superseded ?? 0 }, (_, k) => ({ attempt: k, agent: `old-${k}`, state: "failed" as const })),
  }));
  return { correlation_id: "S", tasks, done: o.results !== false, lastSeq: 999 };
}
const states = (l: any) => l.tasks.map((t: any) => t.state);

describe("statusLine shrink levels", () => {
  test("small story: full claims with artifacts", () => {
    const l: any = statusLine(storyOf(2, { artifacts: 3 }));
    expect([l.correlation_id, l.done, l.lastSeq, l.tasks[0].result.artifacts.length, l.tasks[0].result.summary]).toEqual(["S", true, 999, 3, "done"]);
  });
  test("step 2: artifacts go first, artifact_count and summaries stay", () => {
    const l: any = statusLine(storyOf(12, { artifacts: 20, ref: "x".repeat(199) }));
    expect(lineBytes(l)).toBeLessThanOrEqual(LINES_BUDGET);
    expect(l.tasks.every((t: any) => t.result.artifacts === undefined && t.result.artifact_count === 20 && t.result.summary === "done")).toBe(true);
  });
  test("step 3: summaries and superseded lists go, counts and states stay", () => {
    const l: any = statusLine(storyOf(30, { summary: "Ж".repeat(500), superseded: 3 }));
    expect(lineBytes(l)).toBeLessThanOrEqual(LINES_BUDGET);
    expect(l.tasks.every((t: any) => t.result.summary === undefined && t.result.outcome === "completed" && t.superseded_count === 3 && "lastEventSeq" in t)).toBe(true);
    expect(typeof l.lastSeq).toBe("number");
  });
  test("step 4: compact tasks keep id, state, attempt and assignee", () => {
    const l: any = statusLine(storyOf(300, { results: false, assignee: "w-" + "a".repeat(55) }));
    expect(lineBytes(l)).toBeLessThanOrEqual(LINES_BUDGET);
    expect(l.tasks_omitted).toBeUndefined();
    expect(l.tasks).toHaveLength(300);
    expect("lastEventSeq" in l.tasks[0]).toBe(false);
    expect(states(l).every((s: string) => s === "assigned")).toBe(true);
  });
  test("last resort: tasks are cut, the line says how many, states and lastSeq stay", () => {
    const l: any = statusLine(storyOf(900, { results: false, assignee: "w-" + "a".repeat(55) }));
    expect(lineBytes(l)).toBeLessThanOrEqual(LINES_BUDGET);
    expect(l.tasks.length + l.tasks_omitted).toBe(900);
    expect(l.tasks_omitted).toBeGreaterThan(0);
    expect(states(l).every((s: string) => s === "assigned")).toBe(true);
    expect([typeof l.lastSeq, l.done]).toEqual(["number", false]);
  });
  test("the budget is inclusive: exactly at it keeps the level, one byte under shrinks", () => {
    const st = storyOf(3, { artifacts: 5 });
    const full: any = statusLine(st, 10 * LINES_BUDGET);
    expect(full.tasks[0].result.artifacts).toHaveLength(5);
    expect((statusLine(st, lineBytes(full)) as any).tasks[0].result.artifacts).toHaveLength(5);
    expect((statusLine(st, lineBytes(full) - 1) as any).tasks[0].result.artifacts).toBeUndefined();
  });
  test("the cut form is inclusive too: a budget equal to the cut line keeps every listed task", () => {
    const big = storyOf(900, { results: false, assignee: "w-" + "a".repeat(55) });
    const l: any = statusLine(big, LINES_BUDGET);
    const again: any = statusLine(big, lineBytes(l));
    expect([again.tasks.length, again.tasks_omitted]).toEqual([l.tasks.length, l.tasks_omitted]);
  });
  test("F3: refs are cut by code point, never inside a surrogate pair", () => {
    const st = storyOf(1, { artifacts: 1, ref: "x" + "😀".repeat(300) }); // odd offset: a UTF-16 cut at 200 would split a pair
    const ref: string = (statusLine(st) as any).tasks[0].result.artifacts[0].ref;
    expect(/[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/.test(ref)).toBe(false);
    expect(Array.from(ref)).toHaveLength(200);
  });
  test("G1: even the largest story fits the smallest share (50 stories), as a header with tasks_omitted", () => {
    const share = Math.floor(LINES_BUDGET / 50);
    const big = { ...storyOf(900, { results: false, assignee: "w-" + "a".repeat(55) }), correlation_id: "S".repeat(64) };
    const l: any = statusLine(big, share);
    expect(lineBytes(l)).toBeLessThanOrEqual(share);
    expect([l.correlation_id, l.tasks.length + l.tasks_omitted]).toEqual(["S".repeat(64), 900]);
  });
  test("G1: a smaller budget per story shrinks the line to fit it", () => {
    const budget = Math.floor(LINES_BUDGET / 3);
    const l: any = statusLine(storyOf(12, { artifacts: 20, ref: "x".repeat(199) }), budget);
    expect(lineBytes(l)).toBeLessThanOrEqual(budget);
    expect(l.tasks).toHaveLength(12);
  });
});

describe("heartbeatFrom", () => {
  const NOW = Date.parse("2026-11-07T10:00:00.000Z");
  const hbEv = (o: Record<string, unknown> = {}) => ({
    event_id: "h", timestamp: new Date(NOW).toISOString(), session_id: "s", project_id: "P", correlation_id: "S", task_id: "T",
    agent_id: "A", event_type: "heartbeat", tags: [], to_agent: null, to_task: null,
    payload: { summary: "alive", details: { attempt: 1, seq_of_task_started: 1 } }, ...o,
  });
  test("a well-formed heartbeat at the current time counts", () =>
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv(), NOW)).toEqual({ agent_id: "A", timestamp: new Date(NOW).toISOString() }));
  test("the skew limit is exactly 5 s: NOW+5000 counts, NOW+5001 does not", () => {
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv({ timestamp: new Date(NOW + 5000).toISOString() }), NOW)).toBeDefined();
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv({ timestamp: new Date(NOW + 5001).toISOString() }), NOW)).toBeUndefined();
  });
  test("a heartbeat without a story is ignored, even on the matching 'null' subject", () =>
    expect(heartbeatFrom("amb.hb.P.null.T.A", hbEv({ correlation_id: null }), NOW)).toBeUndefined());
  test("a few seconds of clock skew is tolerated, 30 s in the future is not", () => {
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv({ timestamp: new Date(NOW + 3000).toISOString() }), NOW)).toBeDefined();
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv({ timestamp: new Date(NOW + 30_000).toISOString() }), NOW)).toBeUndefined();
  });
  test("a non-heartbeat event on a heartbeat subject is ignored", () =>
    expect(heartbeatFrom("amb.hb.P.S.T.A", hbEv({ event_type: "finding", payload: { summary: "x" } }), NOW)).toBeUndefined());
  test("a heartbeat without a task is ignored, even on the matching 'null' subject", () =>
    expect(heartbeatFrom("amb.hb.P.S.null.A", hbEv({ task_id: null }), NOW)).toBeUndefined());
  test("subject must match the event's own fields", () =>
    expect(heartbeatFrom("amb.hb.P.ZZ.T.A", hbEv(), NOW)).toBeUndefined());
  test("not an event at all", () => expect(heartbeatFrom("amb.hb.P.S.T.A", { agent_id: "A", timestamp: "x" }, NOW)).toBeUndefined());
});

describe("orchestrator events (flow engine)", () => {
  const by = (agent: string, attempt: number, to: string, at: number, task = "T1") =>
    ev("task_assigned", agent, at, { task_id: task, to_agent: to, details: { brief_ref: `r/${task}`, attempt } });
  const noSeq = (ss: ReturnType<typeof deriveStatus>) => ss.map(({ lastSeq: _, ...rest }) => rest);

  test("S-1 an orchestrator's lifecycle for the assignee it assigned attaches to that attempt", () => {
    const evs = [
      by("orch", 1, "w-T1-1", 0),
      ev("task_started", "orch", 1000, { to_agent: "w-T1-1" }),
      ev("task_result", "orch", 2000, { to_agent: "w-T1-1", details: { outcome: "completed", attempt: 1 } }),
    ];
    expect(one(evs, [], 3000)).toMatchObject({ state: "completed", assignee: "w-T1-1", attempt: 1 });
  });

  test("S-2 a lifecycle event from someone who did not assign that attempt is ignored", () => {
    const evs = [by("orch", 1, "w-T1-1", 0), ev("task_result", "w-T2-1", 10, { to_agent: "w-T1-1", details: { outcome: "completed", attempt: 1 } })];
    expect(one(evs, [], 100)).toMatchObject({ state: "assigned", assignee: "w-T1-1" });
  });

  test("S-3 flow events change nothing but the story's lastSeq; a story with only flow events is not listed", () => {
    const core = [
      by("orch", 1, "w-T1-1", 0),
      ev("task_started", "w-T1-1", 1000),
      ev("task_result", "w-T1-1", 2000, { details: { outcome: "completed", attempt: 1 } }),
    ];
    const flow = [
      ev("gate_pending", "orch", 500, { task_id: "1-loop", details: { gate_id: "1-loop#1.1" } }),
      ev("gate_decided", "orch", 1500, { task_id: "1-loop", details: { gate_id: "1-loop#1.1", decision: "approve" } }),
      ev("status_changed", "orch", 1600, { task_id: null, details: { from: "assigned", to: "started" } }),
      ev("child_link", "orch", 1700, { task_id: null, details: { parent: "S-0" } }),
      ev("kill", "orch", 1800, { task_id: null }),
    ];
    const withFlow = [...core.slice(0, 1), ...flow, ...core.slice(1)];
    const without = deriveStatus(core, [], T0 + 3000, th);
    const mixed = deriveStatus(withFlow, [], T0 + 3000, th);
    expect(noSeq(mixed)).toEqual(noSeq(without));
    expect(mixed[0]!.lastSeq).toBeGreaterThan(without[0]!.lastSeq);
    expect(deriveStatus([ev("kill", "orch", 0, { correlation_id: "S-9", task_id: null })], [], T0, th)).toEqual([]);
  });

  test("S-4 a retry through the orchestrator supersedes the failed attempt", () => {
    const evs = [
      by("orch", 1, "x-a1", 0),
      ev("task_started", "orch", 1000, { to_agent: "x-a1" }),
      ev("error", "orch", 2000, { to_agent: "x-a1", payload: { summary: "boom" } }),
      by("orch", 2, "x-a2", 3000),
    ];
    expect(one(evs, [], 4000)).toMatchObject({
      attempt: 2, assignee: "x-a2", state: "assigned",
      superseded: [{ attempt: 1, agent: "x-a1", state: "failed" }],
    });
  });
});
