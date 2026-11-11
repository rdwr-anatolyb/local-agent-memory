import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { AckPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";
import { busMessages, busOverview, parseCursorName } from "../src/inspect";
import { NatsHarness } from "./harness";

setDefaultTimeout(60_000);

const h = new NatsHarness();
const DAY_MS = 90 * 24 * 3600_000;

async function withNc<T>(f: (nc: NatsConnection) => Promise<T>): Promise<T> {
  const nc = await connect({ servers: h.url });
  try {
    return await f(nc);
  } finally {
    await nc.close();
  }
}
const consumerNames = () => withNc(async (nc) => {
  const names: string[] = [];
  for await (const c of (await jetstreamManager(nc)).consumers.list("AMB_EVENTS")) names.push(c.name);
  return names.sort();
});

beforeAll(async () => {
  await h.start();
  // alpha: a task_started (which also sends one heartbeat), then two findings. beta: two findings.
  const alpha = await h.open({ AMB_PROJECT: "alpha", AMB_AGENT_ID: "sdlc-flow", AMB_STORY: "s1", AMB_TASK: "t1" });
  await alpha.publish({ event_type: "task_started", summary: "go" });
  await alpha.publish({ event_type: "finding", summary: "a1" });
  await alpha.publish({ event_type: "finding", summary: "a2" });
  await alpha.close();
  const beta = await h.open({ AMB_PROJECT: "beta", AMB_AGENT_ID: "sdlc-flow" });
  await beta.publish({ event_type: "finding", summary: "b1" });
  await beta.publish({ event_type: "finding", summary: "b2" });
  await beta.close();
});
afterAll(() => h.dispose());

describe("parseCursorName", () => {
  test("reads project and agent from the length-prefixed name, and rejects other shapes", () => {
    expect(parseCursorName("cursor_5_alpha_sdlc-flow")).toEqual({ project: "alpha", agent: "sdlc-flow" });
    expect(parseCursorName("cursor_3_a_b_c")).toEqual({ project: "a_b", agent: "c" });
    expect(parseCursorName("cursor_5_beta_sdlc-flow")).toBeUndefined();
    expect(parseCursorName("cursor_4_beta_")).toBeUndefined();
    expect(parseCursorName("other_4_beta_x")).toBeUndefined();
  });
});

describe("busOverview", () => {
  test("counts the stored events and heartbeats and reports the retention", async () => {
    const o = await busOverview({ url: h.url });
    expect(o.events.messages).toBe(5);
    expect(o.events.firstSeq).toBe(1);
    expect(o.events.lastSeq).toBe(5);
    expect(o.events.maxAgeMs).toBe(DAY_MS);
    expect(o.events.firstTime).not.toBeNull();
    expect(o.heartbeats.messages).toBe(1);
    expect(typeof o.version).toBe("string");
  });

  test("lists durable cursors only, with project and agent parsed and unread counts", async () => {
    // An ephemeral consumer (no durable name) must never show as a reader.
    await withNc(async (nc) => (await jetstreamManager(nc)).consumers.add("AMB_EVENTS", { ack_policy: AckPolicy.None }));
    const o = await busOverview({ url: h.url });
    const byName = Object.fromEntries(o.readers.map((r) => [r.name, r]));
    expect(o.readers.map((r) => r.name).sort()).toEqual(["cursor_4_beta_sdlc-flow", "cursor_5_alpha_sdlc-flow"]);
    expect(byName["cursor_5_alpha_sdlc-flow"]).toMatchObject({ project: "alpha", agent: "sdlc-flow", pending: 3 });
    expect(byName["cursor_4_beta_sdlc-flow"]).toMatchObject({ project: "beta", agent: "sdlc-flow", pending: 2 });
  });
});

describe("busMessages", () => {
  test("filters by project and lists newest first", async () => {
    const alpha = await busMessages({ url: h.url, project: "alpha" });
    expect(alpha.rows.map((r) => r.project)).toEqual(["alpha", "alpha", "alpha"]);
    expect(alpha.rows.map((r) => r.seq)).toEqual([3, 2, 1]);
    const all = await busMessages({ url: h.url });
    expect(all.rows.map((r) => r.seq)).toEqual([5, 4, 3, 2, 1]);
  });

  test("pages with limit and beforeSeq: every alpha event exactly once", async () => {
    const seen: number[] = [];
    let before: number | undefined;
    for (let i = 0; i < 10; i++) {
      const page = await busMessages({ url: h.url, project: "alpha", limit: 2, beforeSeq: before });
      seen.push(...page.rows.map((r) => r.seq));
      if (page.nextBefore === null) break;
      before = page.nextBefore;
    }
    expect(seen).toEqual([3, 2, 1]);
  });

  test("leaves no temporary consumer behind", async () => {
    const before = await consumerNames();
    await busMessages({ url: h.url });
    await busMessages({ url: h.url, project: "alpha", limit: 1 });
    await busMessages({ url: h.url, project: "beta", beforeSeq: 4 });
    expect(await consumerNames()).toEqual(before);
  });

  describe("trimming, invalid rows and validation", () => {
    beforeAll(async () => {
      const planner = await h.open({ AMB_PROJECT: "gamma", AMB_AGENT_ID: "planner", AMB_ROLE: "planner" });
      await planner.publish({
        event_type: "task_assigned", summary: "assigned", correlation_id: "s1", task_id: "t1", to_agent: "w1",
        details: { brief_ref: "sdlc-flow://s1/t1", attempt: 1 },
      });
      await planner.close();
      const agent = await h.open({ AMB_PROJECT: "gamma", AMB_AGENT_ID: "w1" });
      await agent.publish({ event_type: "finding", summary: "x".repeat(500), details: { body: "y".repeat(40_000) } });
      await agent.close();
    });

    test("cuts summaries to 160 characters and drops free-form details, keeps pointer details", async () => {
      const { rows } = await busMessages({ url: h.url, project: "gamma" });
      const finding = rows.find((r) => r.event === "finding")!;
      const assigned = rows.find((r) => r.event === "task_assigned")!;
      expect(finding.summary.length).toBe(160);
      expect(finding.detail).toBe("");
      expect(assigned.detail).toContain("brief_ref");
    });

    test("skips a message that is not valid JSON and counts it in invalid", async () => {
      await withNc(async (nc) => jetstream(nc).publish("amb.ev.gamma._._.w1.finding", "not json"));
      const { rows, invalid } = await busMessages({ url: h.url, project: "gamma" });
      expect(invalid).toBe(1);
      expect(rows.map((r) => r.event)).toEqual(["finding", "task_assigned"]);
    });

    test("a bad project token throws before any connection is made", async () => {
      // Port 1 refuses connections: the error must be the token check, not a connection failure.
      await expect(busMessages({ url: "nats://127.0.0.1:1", project: "a.b" })).rejects.toThrow("invalid project token");
    });
  });
});
