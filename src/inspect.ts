import { connect } from "@nats-io/transport-node";
import { DeliverPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";
import { EVENTS, HB, parseStored, withOrdered } from "./bus";
import { filterFor, isToken, type EventType, type StoredEvent } from "./event";

/** Messages are read from at most this many seqs below the window's end, so one call costs the same on any stream. */
const SCAN = 2000;
const BATCH = 500;
const SUMMARY_MAX = 160;
const DETAIL_MAX = 200;
/** Only these types have a strict details schema; free-form details (a note's body) never leave amb. */
const POINTER_TYPES: ReadonlySet<EventType> = new Set([
  "task_assigned", "task_result", "heartbeat", "kill", "gate_pending", "gate_decided", "status_changed", "child_link",
]);

// Read-only inspection of the bus for a status page. Nothing here opens a MemoryBus: that would create a cursor.

export type Reader = { name: string; project: string | null; agent: string | null; pending: number; ackPending: number; deliveredSeq: number };
export type BusOverview = {
  version: string | null;
  events: { messages: number; bytes: number; firstSeq: number; lastSeq: number; firstTime: string | null; lastTime: string | null; maxAgeMs: number };
  heartbeats: { messages: number };
  /** Durable consumers of AMB_EVENTS only: each identity's cursor. Ephemeral and ordered consumers are never listed. */
  readers: Reader[];
};

/** The project and agent of a cursor named `cursor_<n>_<project>_<agentId>` (n = the project's length), or undefined. */
export function parseCursorName(name: string): { project: string; agent: string } | undefined {
  const m = /^cursor_(\d+)_/.exec(name);
  if (!m || String(Number(m[1])) !== m[1]) return undefined;
  const n = Number(m[1]);
  const start = m[0].length;
  const project = name.slice(start, start + n);
  const rest = name.slice(start + n);
  return project.length === n && rest.length > 1 && rest.startsWith("_") ? { project, agent: rest.slice(1) } : undefined;
}

export type MessageRow = {
  seq: number; time: string; project: string; story: string | null; task: string | null; agent: string;
  event: EventType; summary: string; detail: string;
};

/**
 * The newest `limit` events (of one project, or all) before `beforeSeq`, newest first. Reads a window of at most
 * SCAN seqs, so `nextBefore` may be non-null for a page with few or no rows: keep paging until it is null.
 * Creates one ordered consumer and deletes it before the connection closes.
 */
export async function busMessages({ url, project, beforeSeq, limit = 50 }: {
  url: string; project?: string; beforeSeq?: number; limit?: number;
}): Promise<{ rows: MessageRow[]; nextBefore: number | null; invalid: number }> {
  if (project !== undefined && !isToken(project)) throw new Error(`invalid project token: ${JSON.stringify(project)}`);
  const want = Math.min(200, Math.max(1, Math.trunc(limit)));
  const nc = await connect({ servers: url, reconnect: false, timeout: 2000 });
  try {
    const jsm = await jetstreamManager(nc);
    const { first_seq, last_seq, messages } = (await jsm.streams.info(EVENTS)).state;
    if (messages === 0) return { rows: [], nextBefore: null, invalid: 0 };
    const end = Math.min(beforeSeq ?? last_seq + 1, last_seq + 1);
    const start = Math.max(first_seq, end - SCAN);
    if (start >= end) return { rows: [], nextBefore: null, invalid: 0 };

    const found: StoredEvent[] = [];
    let invalid = 0;
    await withOrdered(jetstream(nc), EVENTS, {
      filter_subjects: [project === undefined ? "amb.ev.>" : filterFor({ project_id: project })],
      deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: start,
    }, async (c) => {
      for (;;) {
        const pending = (await c.info()).num_pending;
        if (pending === 0) return;
        let got = 0;
        for await (const m of await c.fetch({ max_messages: Math.min(BATCH, pending), expires: 2000 })) {
          got++;
          if (m.seq >= end) return;
          try {
            found.push(parseStored(m));
          } catch {
            invalid++;
          }
        }
        if (got === 0) return;
      }
    });

    const page = found.slice(-want).reverse();
    const nextBefore = found.length >= want ? page.at(-1)!.seq : start > first_seq ? start : null;
    const rows = page.map((e): MessageRow => {
      const details = e.payload.details === undefined ? undefined : JSON.stringify(e.payload.details);
      return {
        seq: e.seq, time: e.timestamp, project: e.project_id, story: e.correlation_id, task: e.task_id, agent: e.agent_id,
        event: e.event_type, summary: e.payload.summary.slice(0, SUMMARY_MAX),
        detail: POINTER_TYPES.has(e.event_type) ? (details?.slice(0, DETAIL_MAX) ?? "") : "",
      };
    });
    return { rows, nextBefore, invalid };
  } finally {
    await nc.close();
  }
}

export async function busOverview({ url }: { url: string }): Promise<BusOverview> {
  const nc = await connect({ servers: url, reconnect: false, timeout: 2000 });
  try {
    const jsm = await jetstreamManager(nc);
    const [ev, hb] = await Promise.all([jsm.streams.info(EVENTS), jsm.streams.info(HB)]);
    const readers: Reader[] = [];
    for await (const c of jsm.consumers.list(EVENTS)) {
      if (!c.config.durable_name) continue;
      const id = parseCursorName(c.name);
      readers.push({
        name: c.name, project: id?.project ?? null, agent: id?.agent ?? null,
        pending: c.num_pending, ackPending: c.num_ack_pending, deliveredSeq: c.delivered.stream_seq,
      });
    }
    const s = ev.state;
    return {
      version: nc.info?.version ?? null,
      events: {
        messages: s.messages, bytes: s.bytes, firstSeq: s.first_seq, lastSeq: s.last_seq,
        firstTime: s.messages ? s.first_ts : null, lastTime: s.messages ? s.last_ts : null,
        maxAgeMs: Number(ev.config.max_age) / 1e6,
      },
      heartbeats: { messages: hb.state.messages },
      readers,
    };
  } finally {
    await nc.close();
  }
}
