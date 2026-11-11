import { connect } from "@nats-io/transport-node";
import { nanos, type NatsConnection } from "@nats-io/nats-core";
import {
  AckPolicy, DeliverPolicy, DiscardPolicy, jetstream, jetstreamManager, RetentionPolicy, StorageType,
  type Consumer, type ConsumerConfig, type JetStreamClient, type JetStreamManager, type JsMsg,
  type OrderedConsumerOptions, type StreamConfig,
} from "@nats-io/jetstream";
import {
  AmbEvent, FLOW_EVENTS, filterFor, hbSubject, LIFECYCLE, MAX_EVENT_BYTES, subjectFor,
  type AmbEventT, type EventType, type StoredEvent,
} from "./event";
import type { Identity } from "./identity";
import { z } from "zod";
import { LINES_BUDGET, lineBytes } from "./untrusted";
import { deriveStatus, type Heartbeat, type StoryStatus } from "./status";

export const EVENTS = "AMB_EVENTS";
export const HB = "AMB_HB";
/** Largest accepted event, measured as the escaped line a reader is sent, so every stored event fits one result. */
export const MAX_EVENT_LINE = 48_000;
const MAX_STORIES = 50;
const HB_CLOCK_SKEW_MS = 5_000;
const DAY = 24 * 3600_000;

type NamedStream = Partial<StreamConfig> & { name: string };
const STREAMS: NamedStream[] = [
  {
    name: EVENTS, subjects: ["amb.ev.>"], storage: StorageType.File, retention: RetentionPolicy.Limits,
    discard: DiscardPolicy.Old, max_age: nanos(90 * DAY), max_bytes: 1024 ** 3, max_msg_size: MAX_EVENT_BYTES,
    duplicate_window: nanos(DAY), num_replicas: 1,
  },
  {
    name: HB, subjects: ["amb.hb.>"], storage: StorageType.File, max_msgs_per_subject: 1, max_age: nanos(3600_000),
    max_msg_size: 4096, num_replicas: 1,
  },
];

export type PublishInput = {
  event_type: EventType; summary: string; details?: unknown; task_id?: string; correlation_id?: string;
  to_agent?: string; tags?: string[]; event_id?: string;
};
export type Query = {
  project_id?: string; correlation_id?: string; task_id?: string; agent_id?: string; event_type?: EventType;
  to_agent?: string; tags_any?: string[]; tags_all?: string[]; fromSeq?: number; fromTime?: string; limit?: number;
  /** Use this agent's cursor filter (its task plus story broadcasts) instead of the project/story/task fields. */
  ownScope?: boolean;
};
export type ReadResult = {
  events: StoredEvent[];
  invalid: { seq: number; error: string }[];
  /** More events were ready but did not fit in LINES_BUDGET; read again. */
  truncated: boolean;
  /**
   * replay_events from_seq=first_seq own_scope=true re-reads this batch if it is lost. After a crash a
   * redelivered older event can make first_seq lower than the previous next_seq.
   */
  first_seq?: number;
  last_seq?: number;
  next_seq?: number;
  /** Seq range of the previous batch that this call ACKed. */
  confirmed?: { first: number; last: number };
};
type StatusQuery = { correlation_id?: string; since?: string; sinceSeq?: number; waitMs?: number };
type Held = { m: JsMsg; eventId: string; seq: number };
export type TaskContext = { task_id: string; by_type: Partial<Record<EventType, StoredEvent[]>> };

/** event_id is chosen by the publisher, so it is unique only per project and agent. */
const dedupeKey = (e: Pick<AmbEventT, "project_id" | "agent_id" | "event_id">) => `${e.project_id}:${e.agent_id}:${e.event_id}`;
/** zod issues as `path code`, never echoing input; our own custom messages carry no input either. */
const issuesText = (e: z.ZodError) =>
  e.issues.map((i) => `${i.path.join(".") || "(root)"} ${i.code}${i.code === "custom" ? ` (${i.message})` : ""}`).join("; ").slice(0, 300);
/** Why a stored message is unreadable, without echoing any of its text. */
const why = (e: unknown) => (e instanceof z.ZodError ? `schema: ${issuesText(e)}` : e instanceof Error && e.message === "subject" ? "subject does not match the event" : "not valid JSON");
/** Every temporary ordered consumer goes through here so it is deleted, never left for inactive_threshold. */
export async function withOrdered<T>(js: JetStreamClient, stream: string, opts: Partial<OrderedConsumerOptions>, f: (c: Consumer) => Promise<T>): Promise<T> {
  const c = await js.consumers.get(stream, opts);
  try {
    return await f(c);
  } finally {
    await c.delete().catch(() => {});
  }
}

/** A stored message as an event, or a throw: valid JSON, the schema, and a subject that agrees with the event's own fields. */
export function parseStored(m: JsMsg): StoredEvent {
  const ev = AmbEvent.parse(m.json());
  if (m.subject !== subjectFor(ev)) throw new Error("subject");
  return { ...ev, seq: m.seq };
}
const sameSet = (a: string[], b: string[]) => a.length === b.length && [...a].sort().join() === [...b].sort().join();

/** `24h`, `30m`, `90s`, `7d` or an ISO timestamp. */
export function parseSince(since: string, now = Date.now()): string {
  const m = /^(\d+)([smhd])$/.exec(since);
  if (m) return new Date(now - Number(m[1]) * { s: 1e3, m: 6e4, h: 36e5, d: 864e5 }[m[2] as "s"]).toISOString();
  if (Number.isNaN(Date.parse(since))) throw new Error(`since must be like 24h or an ISO time, got ${since}`);
  return new Date(since).toISOString();
}

/**
 * Heartbeats are untrusted too: a strict heartbeat event, filed under its own story/task/agent subject,
 * and not from the future (beyond a little clock skew). Anything else is ignored.
 */
export function heartbeatFrom(subject: string, data: unknown, now: number): Heartbeat | undefined {
  const r = AmbEvent.safeParse(data);
  if (!r.success) return undefined;
  const e = r.data;
  if (e.event_type !== "heartbeat" || !e.correlation_id || !e.task_id) return undefined;
  if (subject !== hbSubject(e.project_id, e.correlation_id, e.task_id, e.agent_id)) return undefined;
  if (Date.parse(e.timestamp) > now + HB_CLOCK_SKEW_MS) return undefined;
  return { agent_id: e.agent_id, timestamp: e.timestamp };
}

export class MemoryBus {
  /** Config drift and other non-fatal problems found at open. */
  readonly warnings: string[] = [];
  private hbTimer?: ReturnType<typeof setInterval>;
  /** Bumped by every stop; a start that awaited across a stop sees a newer value and gives up. */
  private hbGen = 0;
  private hbActive = false;
  private readonly sessionId = Bun.randomUUIDv7();
  /** Delivered by the last read() and ACKed at the start of the next one, so a lost batch is redelivered. */
  private held: Held[] = [];
  /** event_ids ACKed by this process; drops redeliveries the server owes after losing an ACK. */
  private readonly returned = new Set<string>();

  private constructor(
    readonly id: Identity,
    private readonly nc: NatsConnection,
    private readonly js: JetStreamClient,
    private readonly jsm: JetStreamManager,
  ) {}

  /** Length-prefixed so `a_b`+`c` and `a`+`b_c` never share a cursor. */
  get cursorName() {
    return `cursor_${this.id.projectId.length}_${this.id.projectId}_${this.id.agentId}`;
  }

  static async open(id: Identity, o: { url?: string } = {}): Promise<MemoryBus> {
    const nc = await connect({ servers: o.url ?? id.url, name: id.agentId, maxReconnectAttempts: -1, reconnectTimeWait: 250 });
    const jsm = await jetstreamManager(nc);
    const bus = new MemoryBus(id, nc, jetstream(nc), jsm);
    try {
      for (const cfg of STREAMS) await bus.ensureStream(cfg);
      await bus.ensureCursor();
    } catch (e) {
      await nc.close();
      throw e;
    }
    for (const w of bus.warnings) console.error(`[amb] warning: ${w}`);
    return bus;
  }

  private async ensureStream(cfg: NamedStream) {
    try {
      await this.jsm.streams.add(cfg);
      return;
    } catch (e) {
      const info = await this.jsm.streams.info(cfg.name).catch(() => { throw e; });
      const actual = info.config as unknown as Record<string, unknown>;
      for (const [k, want] of Object.entries(cfg))
        if (JSON.stringify(actual[k]) !== JSON.stringify(want))
          this.warnings.push(`${cfg.name}: ${k} is ${JSON.stringify(actual[k])}, expected ${JSON.stringify(want)}; the existing stream is used as is`);
    }
  }

  private async ensureCursor() {
    const cfg = {
      durable_name: this.cursorName, ack_policy: AckPolicy.Explicit, ack_wait: nanos(this.id.ackWaitMs), max_deliver: -1,
      filter_subjects: this.id.filter, deliver_policy: this.id.cursorStart === "new" ? DeliverPolicy.New : DeliverPolicy.All,
    } satisfies Partial<ConsumerConfig>;
    const existing = await this.jsm.consumers.info(EVENTS, this.cursorName).catch(() => null);
    if (!existing) {
      await this.jsm.consumers.add(EVENTS, cfg); // concurrent first opens with the same config are idempotent
      return;
    }
    const c = existing.config;
    const stored = c.filter_subjects ?? (c.filter_subject ? [c.filter_subject] : []);
    // Changing scope in place would silently skip events below the ack floor (design §3.4).
    if (!sameSet(stored, cfg.filter_subjects) || c.deliver_policy !== cfg.deliver_policy)
      throw new Error(`identity already bound to another scope (${stored.join(", ")}, ${c.deliver_policy}); use a new AMB_AGENT_ID`);
    if (existing.num_waiting > 0) this.warnings.push(`another process may be pulling on ${this.cursorName}`);
    if (c.ack_wait !== cfg.ack_wait || c.max_deliver !== cfg.max_deliver)
      await this.jsm.consumers.update(EVENTS, this.cursorName, { ack_wait: cfg.ack_wait, max_deliver: cfg.max_deliver });
  }

  private build(input: PublishInput): AmbEventT {
    const { id } = this;
    const ev: AmbEventT = {
      event_id: input.event_id ?? Bun.randomUUIDv7(),
      timestamp: new Date().toISOString(),
      session_id: this.sessionId,
      project_id: id.projectId,
      correlation_id: input.correlation_id ?? id.story ?? null,
      task_id: input.task_id ?? id.task ?? null,
      agent_id: id.agentId,
      event_type: input.event_type,
      tags: input.tags ?? [],
      to_agent: input.to_agent ?? null,
      to_task: null, // reserved for stage 3 cross-task routing
      payload: input.details === undefined ? { summary: input.summary } : { summary: input.summary, details: input.details },
    };
    const parsed = AmbEvent.safeParse(ev);
    if (!parsed.success) throw new Error(`invalid event: ${issuesText(parsed.error)}`);
    const line = lineBytes({ ...ev, seq: Number.MAX_SAFE_INTEGER });
    if (line > MAX_EVENT_LINE) throw new Error(`event too large: ${line} bytes once escaped for readers (limit ${MAX_EVENT_LINE}); shorten details or point to a file`);
    return ev;
  }

  async publish(input: PublishInput): Promise<{ event_id: string; seq: number; duplicate: boolean }> {
    const { id } = this;
    if (id.role === "dashboard") throw new Error("role dashboard is read-only; publish_event is refused");
    if (input.event_type === "heartbeat") throw new Error("heartbeat is sent by the amb MCP process itself, not published");
    if (FLOW_EVENTS.has(input.event_type) && id.role !== "orchestrator") throw new Error(`only AMB_ROLE=orchestrator may publish ${input.event_type}`);
    if (input.event_type === "task_assigned" && id.role !== "planner" && id.role !== "orchestrator")
      throw new Error("only AMB_ROLE=planner or orchestrator may publish task_assigned");
    if (input.event_type === "task_assigned" && id.story && (input.correlation_id ?? id.story) !== id.story)
      throw new Error(`a planner bound to a story assigns only in its own story (${id.story})`);
    const ev = this.build(input);
    if (LIFECYCLE.has(ev.event_type)) {
      if (id.role === "orchestrator") {
        // It has no task of its own: it speaks for the assignee it names, and status checks that it assigned that attempt.
        if (!ev.correlation_id || !ev.task_id || !ev.to_agent)
          throw new Error(`${ev.event_type} from an orchestrator needs correlation_id, task_id and to_agent (the attempt's assignee)`);
      } else {
        // Lifecycle events describe the publisher's own work; a third party cannot start, finish or fail someone's task.
        // A no-task identity may report an `error` about itself only (task null, story null or its own).
        if ((!id.story || !id.task) && ev.event_type !== "error")
          throw new Error(`${ev.event_type} needs an identity with AMB_STORY and AMB_TASK; it reports on your own task`);
        if (ev.correlation_id !== (id.story ?? null))
          throw new Error(`${ev.event_type} must be about your own story (${id.story ?? "none"})`);
        if (ev.task_id !== (id.task ?? null)) throw new Error(`${ev.event_type} must be about your own task (${id.task ?? "none"})`);
      }
    }
    // Scoped so two agents (or projects) choosing the same event_id never dedupe each other.
    const pa = await this.js.publish(subjectFor(ev), JSON.stringify(ev), { msgID: dedupeKey(ev) });
    // One orchestrator serves many tasks, so its lifecycle events never start this process's heartbeat.
    // A duplicate task_started (a retry after a restart) still means this process is working on the task.
    if (ev.event_type === "task_started" && !this.hbActive && id.role !== "orchestrator") {
      this.hbActive = true; // reserved before any await, so concurrent starts cannot both get here
      await this.startHeartbeat(ev, pa.seq);
    }
    if (ev.event_type === "task_result" || ev.event_type === "error") this.stopHeartbeat();
    return { event_id: ev.event_id, seq: pa.seq, duplicate: pa.duplicate };
  }

  private async startHeartbeat(started: AmbEventT, seq: number) {
    const gen = this.hbGen;
    const story = started.correlation_id!, task = started.task_id!;
    const mine = await this.replay({ correlation_id: story, task_id: task, event_type: "task_assigned", to_agent: this.id.agentId });
    if (gen !== this.hbGen) return; // stopped (task_result, error or close) while we were looking up the attempt
    const attempt = (mine.at(-1)?.payload.details as { attempt?: number } | undefined)?.attempt ?? 1;
    const subject = hbSubject(this.id.projectId, story, task, this.id.agentId);
    const beat = async () => {
      try {
        const ev = this.build({ event_type: "heartbeat", summary: "alive", correlation_id: story, task_id: task, details: { attempt, seq_of_task_started: seq } });
        await this.js.publish(subject, JSON.stringify(ev));
      } catch (e) {
        console.error(`[amb] heartbeat failed: ${e}`);
      }
    };
    this.hbTimer = setInterval(beat, this.id.heartbeatMs);
    await beat();
  }

  private stopHeartbeat() {
    this.hbGen++;
    this.hbActive = false;
    clearInterval(this.hbTimer);
    this.hbTimer = undefined;
  }

  private remember(eventId: string) {
    this.returned.add(eventId);
    if (this.returned.size > 10_000) this.returned.delete(this.returned.values().next().value!);
  }

  private async ackHeld(): Promise<ReadResult["confirmed"]> {
    let confirmed: ReadResult["confirmed"];
    for (const { m, eventId, seq } of this.held.splice(0)) {
      try {
        await m.ackAck();
        this.remember(eventId);
        confirmed = { first: Math.min(confirmed?.first ?? seq, seq), last: Math.max(confirmed?.last ?? seq, seq) };
      } catch {
        // Not ACKed: the server redelivers it and the next read returns it again.
      }
    }
    return confirmed;
  }

  /**
   * Advances this agent's cursor. The previous batch is ACKed first (the caller came back, so it got it);
   * this batch stays un-ACKed until the next read, so a session that dies mid-batch gets it again.
   * waitMs below 1000 is rounded up to 1000 (JetStream's minimum pull expiry). If `signal` is aborted by the
   * time the fetch ends (the caller cancelled), this call NAKs only the messages it took and returns nothing.
   */
  async read({ max = 100, waitMs = 0, signal }: { max?: number; waitMs?: number; signal?: AbortSignal } = {}): Promise<ReadResult> {
    const out: ReadResult = { events: [], invalid: [], truncated: false, confirmed: await this.ackHeld() };
    const mine: Held[] = []; // this call's deliveries; they join `held` only if the caller gets them
    const c = await this.js.consumers.get(EVENTS, this.cursorName);
    let bytes = 0, wait = waitMs, first = true;
    const count = () => out.events.length + out.invalid.length;
    while (count() < max && !out.truncated) {
      const info = await c.info();
      // A pull larger than what is deliverable waits for `expires`, so ask only for what is ready. Ack-pending
      // (redeliveries a dead session or a cancelled call left behind) is asked for only when nothing new is pending.
      let n = Math.min(max - count(), info.num_pending || (first ? info.num_ack_pending : 0));
      first = false;
      if (n === 0) {
        if (wait <= 0 || count() > 0) break;
        n = 1;
      }
      let got = 0;
      for await (const m of await c.fetch({ max_messages: n, expires: Math.max(1000, wait) })) {
        got++;
        if (out.truncated) {
          m.nak(); // give it back now instead of after ack_wait
          continue;
        }
        let ev: StoredEvent;
        try {
          ev = parseStored(m);
        } catch (e) {
          out.invalid.push({ seq: m.seq, error: why(e) });
          await m.ackAck().catch(() => {});
          continue;
        }
        const key = dedupeKey(ev);
        if (this.returned.has(key)) {
          await m.ackAck().catch(() => {});
          continue;
        }
        const dup = mine.find((x) => x.eventId === key);
        if (dup) {
          dup.m = m; // redelivered within this call: return it once, ACK the latest delivery later
          continue;
        }
        const size = lineBytes(ev);
        if (out.events.length > 0 && bytes + size > LINES_BUDGET) {
          out.truncated = true;
          out.next_seq = m.seq;
          m.nak();
          continue;
        }
        bytes += size;
        out.events.push(ev);
        mine.push({ m, eventId: key, seq: m.seq });
      }
      wait = 0;
      if (got === 0) break;
    }
    if (signal?.aborted) {
      for (const { m } of mine) m.nak();
      return { events: [], invalid: out.invalid, truncated: false, confirmed: out.confirmed };
    }
    this.held.push(...mine);
    const seqs = [...out.events.map((e) => e.seq), ...out.invalid.map((i) => i.seq)];
    if (seqs.length) {
      out.first_seq = Math.min(...seqs);
      out.last_seq = Math.max(...seqs);
      out.next_seq ??= out.last_seq + 1;
    }
    return out;
  }

  private withOrdered<T>(stream: string, opts: Partial<OrderedConsumerOptions>, f: (c: Consumer) => Promise<T>): Promise<T> {
    return withOrdered(this.js, stream, opts, f);
  }

  /** Reads everything an ordered consumer can see right now. */
  private async drain(c: Consumer): Promise<JsMsg[]> {
    const msgs: JsMsg[] = [];
    let pending = (await c.info()).num_pending;
    while (pending > 0) {
      const before = msgs.length;
      for await (const m of await c.fetch({ max_messages: Math.min(pending, 1000), expires: 2000 })) {
        msgs.push(m);
        pending = m.info.pending;
        if (pending === 0) break;
      }
      if (msgs.length === before) break;
    }
    return msgs;
  }

  async replay(q: Query): Promise<StoredEvent[]> {
    const filters = q.ownScope ? this.id.filter
      : [filterFor({ project_id: q.project_id ?? this.id.projectId, correlation_id: q.correlation_id, task_id: q.task_id, agent_id: q.agent_id, event_type: q.event_type })];
    const msgs = await this.withOrdered(EVENTS, {
      filter_subjects: filters,
      ...(q.fromSeq !== undefined ? { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: Math.max(1, q.fromSeq) }
        : q.fromTime !== undefined ? { deliver_policy: DeliverPolicy.StartTime, opt_start_time: parseSince(q.fromTime) }
        : { deliver_policy: DeliverPolicy.All }),
    }, (c) => this.drain(c));
    const seen = new Set<string>();
    const events = msgs.flatMap((m): StoredEvent[] => {
      let e: StoredEvent;
      try {
        e = parseStored(m);
      } catch {
        return []; // unreadable or mis-filed messages are skipped, as read() reports them
      }
      const key = dedupeKey(e);
      if (seen.has(key)) return [];
      seen.add(key);
      const keep = (!q.to_agent || e.to_agent === q.to_agent)
        && (!q.tags_any?.length || q.tags_any.some((t) => e.tags.includes(t)))
        && (!q.tags_all?.length || q.tags_all.every((t) => e.tags.includes(t)));
      return keep ? [e] : [];
    });
    if (!q.limit) return events;
    // Paging forward from a point keeps the head; a plain history query wants the most recent.
    return q.fromSeq !== undefined || q.fromTime !== undefined ? events.slice(0, q.limit) : events.slice(-q.limit);
  }

  async getTaskContext(taskId: string, { perTypeLimit = 20, correlation_id }: { perTypeLimit?: number; correlation_id?: string } = {}): Promise<TaskContext> {
    const by_type: TaskContext["by_type"] = {};
    for (const e of await this.replay({ correlation_id: correlation_id ?? this.id.story, task_id: taskId })) (by_type[e.event_type] ??= []).push(e);
    for (const k of Object.keys(by_type) as EventType[]) by_type[k] = by_type[k]!.slice(-perTypeLimit);
    return { task_id: taskId, by_type };
  }

  getDecisions(q: Pick<Query, "project_id" | "correlation_id" | "task_id" | "limit">) {
    return this.replay({ ...q, event_type: "decision" });
  }

  private async heartbeats(story?: string): Promise<Heartbeat[]> {
    const msgs = await this.withOrdered(HB, {
      filter_subjects: [`amb.hb.${this.id.projectId}.${story ?? "*"}.>`],
      deliver_policy: DeliverPolicy.LastPerSubject,
    }, (c) => this.drain(c));
    const now = Date.now();
    return msgs.flatMap((m): Heartbeat[] => {
      let data: unknown;
      try {
        data = m.json();
      } catch {
        return [];
      }
      const hb = heartbeatFrom(m.subject, data, now);
      return hb ? [hb] : [];
    });
  }

  /**
   * With waitMs, first waits for a story event after sinceSeq (or, without sinceSeq, for the next new one),
   * returning early when one lands. Then recomputes status from replay + last heartbeats.
   * A named story is always replayed in full. Without a story, events since `since` (default: the status
   * window, 24h) only pick up to 50 active stories, which are then replayed in full.
   */
  async getStatus(q: StatusQuery = {}): Promise<StoryStatus[]> {
    return (await this.statusPage(q)).stories;
  }

  /** getStatus plus `active`: how many stories had task activity in the window (only the newest 50 are returned). */
  async statusPage(q: StatusQuery = {}): Promise<{ stories: StoryStatus[]; active: number }> {
    if (q.waitMs && q.waitMs > 0) {
      const filter = filterFor({ project_id: this.id.projectId, correlation_id: q.correlation_id });
      const start = q.sinceSeq !== undefined
        ? { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: q.sinceSeq + 1 }
        : { deliver_policy: DeliverPolicy.New };
      await this.withOrdered(EVENTS, { filter_subjects: [filter], ...start }, async (c) => {
        if ((await c.info()).num_pending > 0) return;
        for await (const _ of await c.fetch({ max_messages: 1, expires: Math.max(1000, q.waitMs!) })) break;
      });
    }
    const hbs = await this.heartbeats(q.correlation_id);
    const now = Date.now();
    if (q.correlation_id) {
      const stories = deriveStatus(await this.replay({ correlation_id: q.correlation_id }), hbs, now, this.id.thresholds);
      return { stories, active: stories.length };
    }
    // The window only picks the active stories; each is then replayed in full, so a late result of an old story counts.
    const since = q.since ?? new Date(now - this.id.statusWindowMs).toISOString();
    const active = new Map<string, number>();
    for (const e of await this.replay({ fromTime: since }))
      if (e.correlation_id && e.task_id && (e.event_type === "task_assigned" || LIFECYCLE.has(e.event_type))) active.set(e.correlation_id, e.seq);
    const picked = [...active].sort((a, b) => b[1] - a[1]).slice(0, MAX_STORIES).map(([story]) => story);
    const stories = [];
    for (const story of picked) stories.push(...deriveStatus(await this.replay({ correlation_id: story }), hbs, now, this.id.thresholds));
    return { stories, active: active.size };
  }

  async cursorStatus() {
    const [ci, si] = await Promise.all([this.jsm.consumers.info(EVENTS, this.cursorName), this.jsm.streams.info(EVENTS)]);
    return {
      filter: this.id.filter,
      ackFloorSeq: ci.ack_floor.stream_seq,
      pending: ci.num_pending,
      streamLastSeq: si.state.last_seq,
      // Approximate: compares against the whole stream, so retention of other scopes can also set it.
      gap: si.state.first_seq > 1 && ci.delivered.stream_seq < si.state.first_seq - 1,
    };
  }

  /** Stops the heartbeat. The last batch stays un-ACKed on purpose: the session may not have used it. */
  async close() {
    this.stopHeartbeat();
    if (this.nc.isClosed()) return;
    // drain waits for a PONG; a connection that lost its server never gets one, so close it after a bound.
    await Promise.race([this.nc.drain().catch(() => {}), Bun.sleep(2000)]);
    if (!this.nc.isClosed()) await this.nc.close();
  }
}
