import { LIFECYCLE, type StoredEvent } from "./event";
import type { Thresholds } from "./identity";

export type TaskState = "assigned" | "started" | "completed" | "failed" | "unclaimed" | "stalled" | "quiet";
export type Heartbeat = { agent_id: string; timestamp: string };
export type TaskStatus = {
  correlation_id: string; task_id: string; attempt: number; assignee: string; state: TaskState;
  lastEventSeq: number; lastHeartbeat?: string; result?: StoredEvent;
  superseded: { attempt: number; agent: string; state: TaskState }[];
};
export type StoryStatus = { correlation_id: string; tasks: TaskStatus[]; done: boolean; lastSeq: number };

/**
 * One attempt = one assignee. `assigned` is false for an ad hoc agent that started a task nobody assigned.
 * `assigner` is the agent whose task_assigned opened the attempt; only it may report lifecycle for the assignee.
 */
type Attempt = { attempt: number; agent: string; assigned: boolean; assigner: string; events: StoredEvent[] };

const has = (a: Attempt, t: StoredEvent["event_type"]) => a.events.find((e) => e.event_type === t);
const outcome = (e: StoredEvent) => (e.payload.details as { outcome?: string } | undefined)?.outcome;
const ms = (iso: string) => Date.parse(iso);

function stateOf(a: Attempt, hb: Heartbeat | undefined, now: number, th: Thresholds): TaskState {
  const result = has(a, "task_result");
  if (result && outcome(result) === "completed") return "completed";
  if ((result && outcome(result) === "failed") || has(a, "error")) return "failed";
  const started = has(a, "task_started");
  if (started) {
    const alive = ms(hb?.timestamp ?? started.timestamp);
    if (now - alive > th.stallAfterMs) return "stalled";
    const lastEvent = Math.max(...a.events.map((e) => ms(e.timestamp)));
    return hb && now - lastEvent > th.quietAfterMs ? "quiet" : "started";
  }
  const assigned = has(a, "task_assigned");
  return assigned && now - ms(assigned.timestamp) > th.claimTimeoutMs ? "unclaimed" : "assigned";
}

/**
 * Pure projection shared by planner and dashboard. Events must be in seq order.
 * Attempts come only from task_assigned (or a task_started on a task nobody assigned);
 * every other event attaches to the attempt whose assignee sent it, or is ignored.
 */
export function deriveStatus(events: StoredEvent[], heartbeats: Heartbeat[], now: number, th: Thresholds): StoryStatus[] {
  const hbByAgent = new Map(heartbeats.map((h) => [h.agent_id, h]));
  const stories = new Map<string, { lastSeq: number; tasks: Map<string, Map<string, Attempt>> }>();

  for (const e of events) {
    if (!e.correlation_id) continue;
    let story = stories.get(e.correlation_id);
    if (!story) stories.set(e.correlation_id, (story = { lastSeq: 0, tasks: new Map() }));
    story.lastSeq = Math.max(story.lastSeq, e.seq);
    if (!e.task_id) continue;
    let attempts = story.tasks.get(e.task_id);
    const open = (agent: string, attempt: number, assigned: boolean, assigner: string) => {
      if (!attempts) story.tasks.set(e.task_id!, (attempts = new Map()));
      attempts.set(agent, { attempt, agent, assigned, assigner, events: [e] });
    };

    if (e.event_type === "task_assigned" && e.to_agent) {
      const attempt = (e.payload.details as { attempt: number }).attempt;
      const cur = attempts?.get(e.to_agent);
      // A planner retry of the same assignment (new event_id) must not reset a running or finished attempt.
      if (cur?.assigned && cur.attempt === attempt) cur.events.push(e);
      else open(e.to_agent, attempt, true, e.agent_id);
    }
    else if (attempts?.has(e.agent_id)) attempts.get(e.agent_id)!.events.push(e);
    // An orchestrator holds no attempt; it reports for an assignee only if it made that assignment.
    else if (LIFECYCLE.has(e.event_type) && e.to_agent && attempts?.get(e.to_agent)?.assigner === e.agent_id)
      attempts.get(e.to_agent)!.events.push(e);
    // A task_started that names an assignee is that assignee's or nothing: it never opens an attempt for its sender.
    else if (e.event_type === "task_started" && !e.to_agent) open(e.agent_id, 0, false, "");
  }

  return [...stories].map(([correlation_id, s]) => {
    const tasks = [...s.tasks].map(([task_id, all]): TaskStatus => {
      const assignedOnes = [...all.values()].filter((a) => a.assigned);
      // Once a planner assigns the task, ad hoc starters no longer count.
      const sorted = (assignedOnes.length ? assignedOnes : [...all.values()])
        .sort((x, y) => x.attempt - y.attempt || x.events[0]!.seq - y.events[0]!.seq);
      const latest = sorted.at(-1)!;
      const hb = hbByAgent.get(latest.agent);
      return {
        correlation_id, task_id, attempt: latest.attempt, assignee: latest.agent,
        state: stateOf(latest, hb, now, th),
        lastEventSeq: latest.events.at(-1)!.seq,
        lastHeartbeat: hb?.timestamp,
        result: has(latest, "task_result"),
        superseded: sorted.slice(0, -1).map((a) => ({ attempt: a.attempt, agent: a.agent, state: stateOf(a, hbByAgent.get(a.agent), now, th) })),
      };
    });
    return { correlation_id, tasks, done: tasks.length > 0 && tasks.every((t) => t.state === "completed"), lastSeq: s.lastSeq };
  }).filter((s) => s.tasks.length > 0);
}
