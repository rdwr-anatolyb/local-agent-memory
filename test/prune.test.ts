import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { connect } from "@nats-io/transport-node";
import { AckPolicy, jetstreamManager } from "@nats-io/jetstream";
import { pruneCursors } from "../src/prune";
import { NatsHarness } from "./harness";

setDefaultTimeout(60_000);

const h = new NatsHarness();
let malformed = "";
let ephemeral = "";

async function withJsm<T>(f: (jsm: Awaited<ReturnType<typeof jetstreamManager>>) => Promise<T>): Promise<T> {
  const nc = await connect({ servers: h.url });
  try {
    return await f(await jetstreamManager(nc));
  } finally {
    await nc.close();
  }
}
const cursorNames = () => withJsm(async (jsm) => {
  const names: string[] = [];
  for await (const c of jsm.consumers.list("AMB_EVENTS")) names.push(c.name);
  return names.sort();
});
const prune = (keepProjects: string[]) => pruneCursors({ url: h.url, agentId: "sdlc-flow", keepProjects });

beforeAll(async () => {
  await h.start();
  // Opening a bus creates the AMB_EVENTS stream and that identity's durable cursor.
  const identities: [string, string][] = [["alpha", "sdlc-flow"], ["beta", "sdlc-flow"], ["gamma", "sdlc-flow"], ["beta", "w-x-1"]];
  for (const [project, agent] of identities) {
    const bus = await h.open({ AMB_PROJECT: project, AMB_AGENT_ID: agent });
    await bus.close();
  }
  // Same agent, but the length prefix says 5 while the project "beta" is 4 characters long.
  malformed = "cursor_5_beta_sdlc-flow";
  await withJsm((jsm) => jsm.consumers.add("AMB_EVENTS", { durable_name: malformed, ack_policy: AckPolicy.Explicit }));
  ephemeral = (await withJsm((jsm) => jsm.consumers.add("AMB_EVENTS", { ack_policy: AckPolicy.None }))).name;
});
afterAll(() => h.dispose());

describe("pruneCursors", () => {
  test("removes sdlc-flow cursors of projects that are not kept and leaves everything else", async () => {
    expect((await prune(["alpha"])).sort()).toEqual(["beta", "gamma"]);
    const alpha = "cursor_5_alpha_sdlc-flow";
    const betaOther = "cursor_4_beta_w-x-1";
    expect(await cursorNames()).toEqual([alpha, betaOther, malformed, ephemeral].sort());
  });

  test("a second prune removes nothing", async () => {
    expect(await prune(["alpha"])).toEqual([]);
  });

  test("a name with a wrong length prefix is left alone", async () => {
    expect(await cursorNames()).toContain(malformed);
  });
});
