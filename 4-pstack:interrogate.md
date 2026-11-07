# Interrogate review of the Agent Memory Bus stage-1 prototype

**Verdict: request changes before stage 2.** The prototype works mechanically. `bun test` passes 62/62 and `tsc` is clean (reviewers A and B both ran these). The durable cursor, dedupe in the simple case, ordered replay and the bind to 127.0.0.1 all behave as designed. Three problems stop it from being the planner's source of truth:
- Any agent can fake a task's terminal state with one call.
- A reused `event_id` silently drops another agent's event.
- Reads ACK events before the LLM has seen them, and results have no size cap.

None of the fixes needs a design change. They are small changes in `src/bus.ts`, `src/status.ts` and `src/mcp.ts`, plus one proof test for each.

**Reviewers.** All three ran on the same filled `pstack:interrogate` prompt and rubric. A had 12 findings, B had 13 and C had 9.
- **Reviewer A (opus):** bus.ts correctness and test quality.
- **Reviewer B (fable):** trust, security and design gaps.
- **Reviewer C (sonnet):** MCP surface, SKILL.md fit and test quality.

**My own verification.** I reran reviewer B's `repro.ts` and `ackorder.ts`. I also ran reviewer A's `repro.ts` (`collide`, `noredeliver`, `leak`) and `phantom.ts` against my own temporary `nats-server`, with a random port on 127.0.0.1 and a home directory under scratchpad. I stopped that server afterwards. I edited no source files and made no commits.

## Findings ranked by severity

Root: `/Users/anatolyb/.sdlc-flow/worktrees/feature-skill/local-agent-memory-bus-nats-jetstream`.

### H1. High. A worker can fake another task's final state, in another task or another story
- **Where:** `src/bus.ts:106-123`, `src/status.ts:21-22,49-54`, `src/event.ts:25-29`.
- **Evidence:** The role guard covers only `task_assigned` and the `task_id` of a worker's `task_result`. `task_completed` and `error` are also terminal in `stateOf`, but they have no guard and no details schema. `deriveStatus` takes `attempt` from free-form `details` when `a.attempt === 0`. A `task_result` with a `correlation_id` override also passes the guard.
- **Repro** (B's `repro.ts`, which I reran): worker `w-T1-1` published `task_completed {task_id:"T2", details:{attempt:99}}` and `error {task_id:"T3", ...}`. The result was `["T2","completed","w-T1-1",99]` and `["T3","failed","w-T1-1",99]`. A cross-story `task_result {correlation_id:"S2"}` was accepted and marked S2/T1 completed. One call is enough to make `done` read true, and to trigger a needless reassignment through `failed`.
- **Fix:**
  - For lifecycle types (`task_started`, `task_result`, `task_completed`, `error`) from role worker, require `correlation_id === id.story` and `task_id === id.task`. Better, stamp both from the identity.
  - In `deriveStatus`, take `attempt` only from `task_assigned`. Count terminal events only from that attempt's assignee.
  - Consider deleting `task_completed`. It duplicates `task_result` but has no schema and no artifacts.
- **Raised by:** fable and sonnet (consensus).
- **Verified:** yes (repro).

### H2. High. Caller-chosen `event_id` is a stream-wide dedupe key, so one agent's id drops another agent's event in any project
- **Where:** `src/bus.ts:134` (`msgID: ev.event_id`), `src/mcp.ts:42`. SKILL.md tells agents to pass `event_id` when retrying.
- **Repro** (A's `repro.ts collide`, which I reran): project `pa` agent A published `event_id:"finding-1"` and got seq 1. Project `pb` agent B published the same id and got `{seq:1, duplicate:true}`. A replay of `pb` returned 0 events. The window is 24 h, and LLM-invented ids such as `finding-1` will collide.
- **Fix:** use `msgID = ${project}.${agentId}.${event_id}`, or accept only UUIDs. Add a P3 variant with two agents.
- **Raised by:** opus.
- **Verified:** yes (repro).

### H3. High. Events are ACKed before the result reaches the LLM, and results have no byte cap, so the agent can lose events and never know
- **Where:** `src/bus.ts:180-185` (`ackAck` inside the fetch loop), `src/mcp.ts:49-55,64-69`. `details` is `z.unknown()` up to 64 KB per event.
- **Evidence:**
  - `read_events` with `max:100` can return about 6.4 MB, sent twice (in `content` and in `structuredContent`). C measured 1,207,863 bytes for 20 events with 60 KB details.
  - A result that the client truncates or rejects, cancels, or times out has already advanced the cursor. The handler never looks at `extra.signal`. `cursor_status` shows no gap, because the ACK happened.
  - Related, from B's `ackorder.ts` (reran, simulated ack failure): `remember()` runs before `ackAck()`. When the ACK throws, the redelivered event is dropped as "already returned", and the next read gives `events [] invalid []`.
- **Fix:**
  - Defer the ACK by one call (ACK the previous batch at the start of the next `read_events`), or add an explicit `ack_up_to_seq`.
  - Return `first_seq` and `last_seq` (or `next_seq`), so `replay_events(from_seq)` can recover a lost batch.
  - Cap result bytes at about 50 KB with a `truncated` flag and a pointer to `seq` for the rest.
  - Call `remember()` only after a successful ACK.
- **Raised by:** opus, fable and sonnet. All three named the ACK-ordering issue, and C added the size issue.
- **Verified:** partly. The size was measured, the ACK order is clear in the code, and the remember-before-ACK case was shown with a simulated failure. Claude Code's real truncation limit and Codex cancel behaviour were not tested.

### H4. High. Any event that carries a `task_id` creates a fake "assigned" task, so the story's `done` stays false for ever
- **Where:** `src/status.ts:44-54,30-31`.
- **Repro** (A's `phantom.ts`, which I reran): a `finding` with `task_id:"T2"` from the worker of T1 gave `done:false` with tasks `[["T1","w-T1-1","completed"],["T2","w-T1-1","assigned"]]`. P11 itself has the planner publish findings tagged with a task. Planner-authored events on a real task also show up as a `superseded` attempt 0 with state "assigned".
- **Fix:** create attempts only from `task_assigned` (plus `task_started` for ad hoc agents). Attach other events only to an attempt that already exists. This is the same change as the `deriveStatus` part of H1.
- **Raised by:** opus.
- **Verified:** yes (repro).

### M1. Medium. Lifecycle events published by the harness, which SKILL.md allows, break status and heartbeats
- **Where:** `src/status.ts:48-54`, `src/bus.ts:135`, `skills/agent-memory-bus/SKILL.md:40,59-61`.
- **Repro** (B's case D, which I reran): sdlc-flow published `task_started` while the worker was heartbeating. The result was `state:"stalled", assignee:"sdlc-flow"`, and the real worker's attempt was `unclaimed`. The heartbeat also starts in the publisher's process, not in the worker's.
- **Fix:** make lifecycle events self-published only. Say so in SKILL.md and enforce it with the H1 guard. Alternatively, key lifecycle events by `to_agent` and start the heartbeat only when `agent_id === id.agentId && task_id === id.task`.
- **Raised by:** fable and sonnet.
- **Verified:** yes (repro).

### M2. Medium. Ordered consumers are never deleted, so each read call leaves 1 to 3 server consumers alive for 5 minutes
- **Where:** `src/bus.ts:199-235` (`drain`/`replay`), `248-261` (`heartbeats`), `264-275` (`getStatus` wait), `143` (`startHeartbeat`).
- **Repro:** A's `repro.ts leak` (reran) showed 5 consumers before, 30 after 20 replays and 5 `getStatus` calls, and still 30 six seconds later. B's case C showed 6 rising to 46. A planner loop that wakes on every story event, together with dashboards, will pile up thousands of consumers.
- **Fix:** wrap each use in `try { ... } finally { await c.delete().catch(() => {}) }`. One helper that runs a function with a temporary consumer covers all four sites.
- **Raised by:** opus and fable.
- **Verified:** yes (repro).

### M3. Medium. `structuredContent` carries every event result without the untrusted wrapper, and doubles token cost
- **Where:** `src/mcp.ts:16-20`, and the event tools at 54, 68, 76, 84 and 98.
- **Evidence:** the raw events, hostile text included, go out next to the wrapped text. A host that shows `structuredContent` to the model bypasses the preamble and the `<untrusted_events>` envelope. Hosts that show both pay twice. The tests (`test/harness.ts:85`, P8 and P10) assert on the structured copy, so the path the LLM actually reads is barely tested.
- **Fix:** drop `structuredContent` from the event-bearing tools and keep it for `publish_event` and `cursor_status`. Tests can parse the JSON lines from the text.
- **Raised by:** opus, fable and sonnet (all three).
- **Verified:** no for host behaviour. Whether Codex prefers `structuredContent` was not checked. The code path itself is confirmed.

### M4. Medium. The documented agent-id convention collides across stories, and cursor names are ambiguous
- **Where:** README.md (the `w-<task>-<attempt>` row), SKILL.md, `src/bus.ts:62`.
- **Repro:** B's case B (reran): `w-T1-1` in story S2 got "identity already bound to another scope". A's `repro.ts name` showed that project `a_b` with agent `c` and project `a` with agent `b_c` both map to `cursor_a_b_c`. Because `_` is a legal token character, this is denial of service, not mixing of data.
- **Fix:** use the convention `w-<story>-<task>-<attempt>`, and a cursor name that cannot be ambiguous (length-prefixed or hashed).
- **Raised by:** opus and fable.
- **Verified:** yes (repro).

### M5. Medium. `read_events` without `wait_ms` ignores messages that are due for redelivery
- **Where:** `src/bus.ts:175-178`. The batch size comes from `num_pending`, which excludes messages still awaiting an ACK.
- **Repro:** A's `repro.ts noredeliver` (reran): one message fetched but not ACKed, `ack_wait` 500 ms, then a read returned nothing (`ackFloorSeq 0, pending 0`).
- **Fix:** use `num_pending + num_ack_pending`, or always issue one short fetch when n is 0.
- **Raised by:** opus.
- **Verified:** yes (repro and code).

### M6. Medium. Reopening an existing identity fails with a cryptic error if `AMB_ACK_WAIT_MS` or any default changes
- **Where:** `src/bus.ts:88-102`.
- **Repro:** A's `repro.ts ackwait` gave `JetStreamApiError: consumer already exists`. Any future change to a default would break every existing durable.
- **Fix:** call `consumers.update` for mutable fields, and give a clear error only when the filter or `deliver_policy` differs.
- **Raised by:** opus.
- **Verified:** yes (A's repro). I did not rerun it.

### M7. Medium. A missing `AMB_ROLE` defaults to `agent`, the role with the most permissions
- **Where:** `src/identity.ts:54,62-70`, `src/bus.ts:122`.
- **Evidence:** role `agent` gets a project-wide cursor from the start, may publish `task_result` for any task, and may set `AMB_FILTER` to anything. With Codex `env_vars`, an unset variable is dropped silently, so a worker spawned with a bad environment becomes `agent`.
- **Fix:** require `AMB_ROLE` and fail closed. Keep `agent` only when it is set explicitly.
- **Raised by:** fable.
- **Verified:** yes (code read).

### Low
- **L1.** `get_status` with `wait_ms` but no `since_seq` never waits, and `read_events` with `wait_ms` under 1000 never waits (`bus.ts:177,265`). C's repro returned in 2 ms and 4 ms. SKILL.md does not say what `since_seq` to use on the first call. Raised by fable and sonnet. Verified yes.
- **L2.** `get_status` without `correlation_id` or `since` drains the whole project stream (up to 1 GB) on every call. It also returns full `result` events, so its run time is unbounded on top of the 25 s wait (`bus.ts:273`). Fix: default `since` to 24h, cap the size, trim `result`. Raised by sonnet. Verified no (code read, not measured).
- **L3.** `brief_ref` accepts 500 characters of free text (`event.ts:17`), and SKILL.md tells workers to read it. B's case E accepted an injection string. Fix: restrict it to a URI-like reference shape. Raised by fable. Verified yes.
- **L4.** The planner loop in SKILL.md polls only `get_status`. A worker's `question` or `help_request` stays unseen until `quiet` fires after 30 minutes. Raised by fable. Verified yes (code read).
- **L5.** `task_completed` does not stop the heartbeat (`bus.ts:136`). Two `task_started` publishes running at once can leak an interval timer. Raised by opus. Verified yes for the first part (code read). The second part is reasoning only.
- **L6.** `amb server stop` sends SIGTERM to whatever pid a stale `nats.pid` names (`server.ts:48-61`). Raised by fable. Verified yes (code read).
- **L7.** `ensureStream` swallows config drift, so later limit changes have no effect (`bus.ts:79-86`). Raised by fable. Verified yes (code read).
- **L8.** The tool schemas leave the required shapes of `task_assigned` and `task_result` to prose (`details: z.unknown()`). The TRUST paragraph repeats in five descriptions. `to_task` is accepted but nothing reads it. `get_decisions` has no default limit. Raised by sonnet and fable. Verified yes (code read).

### Test-quality findings
- **T1.** P8 (`test/proofs.test.ts:175-184`) does not prove that `get_status` returns early. With a 3 s wait and 30 iterations, it would pass even if the call always waited the full time. Add a latency assertion. Raised by opus. Verified yes (loop bounds).
- **T2.** The superseding proofs (P9 and `status.test.ts:53-64`) cover only a late `failed` result. The dangerous case is untested: a late `completed` result from attempt 1 while attempt 2 is still `started`. Raised by opus. Verified yes (code read).
- **T3.** H1 to H4, M1, M2, M5 and M6 each pass the current suite. P10 passes while H1 and L3 exist. Raised by all three.
- **T4.** Timing margins are tight: P9's 500 ms sleep against a 600 ms stall threshold, the `< 2600` bound, P12's 700 ms sleep and P9b's 1500 ms sleep. These are fine locally but likely flaky under CI load. Use polling with a deadline. Raised by fable and sonnet. Verified by reading only. The suite was not rerun under load.
- **T5.** P7 never exercises `MemoryBus.read` for the case where the result is not ACKed. Nothing tests that heartbeats stop after `task_result` or `close()`. Raised by opus and sonnet.

## Dismissed claims
- **Double ACK when a second process opens the same id** (sonnet). The design requires a unique `AMB_AGENT_ID` for each attempt, and the code already warns when `num_waiting > 0`. This is noted, not a defect.
- **Per-process `returned` set is limited to 10k entries** (sonnet). After a restart, dedupe falls back to JetStream redelivery semantics, which the design accepts as at-least-once. It is not a defect on its own. H2 and H3 cover the real loss paths.
- **`maxReconnectAttempts: -1` makes a call hang when the server is dead** (sonnet). This is plausible but was not reproduced. The client timeout bounds the hang. It is left as a follow-up observation.
- **The 25 s wait plus replay could exceed the Codex 60 s timeout** (sonnet). Sonnet's own estimate is about 27 s at worst. Only the unbounded replay in L2 could break that limit, and L2 covers it.
- **Duplicates:** the cursor-name `_` collision was merged into M4. The wait_ms notes were merged into L1. The result-size notes were merged into H3.

## Agreement map
All three models independently flagged the `structuredContent` bypass (M3) and the problem of ACKing before delivery (H3). fable and sonnet both found the lifecycle forgery (H1) and the harness lifecycle break (M1) with separate repros. opus found the data-loss bugs the other two missed (H2, H4, M5, M6). opus and fable both measured the consumer leak (M2). No reviewer contradicted another. The pattern is consistent: the trust model is enforced only for `task_assigned` and `task_result`, while status derivation trusts every event.

## What was not checked
- How real Claude Code and Codex hosts treat `structuredContent` and oversized results (M3, H3).
- Suite stability under CPU load or across repeated runs.
- The scale behaviour of M2 and L2, such as thousands of consumers or a 1 GB stream.
- A real retention gap for `cursor_status.gap`. A's purge test showed no false positive.
- Whether the server-side `inactive_threshold` cleanup holds when consumer counts get large.

## Recommended next actions
1. **H1, H4, M1.** Stamp story and task for worker lifecycle events. Derive attempts only from `task_assigned`, and count terminal events only from the assignee. Drop or schema `task_completed`. Make lifecycle events self-published in SKILL.md. Add a proof that a forged state is refused and a test for the fake-task case.
2. **H2.** Scope `msgID` to project and agent. Add a two-agent dedupe proof.
3. **H3.** Defer the ACK by one call, or add `ack_up_to_seq`. Return `next_seq`. Cap result bytes with a `truncated` flag. Call `remember()` only after the ACK.
4. **M2.** Delete temporary consumers in `finally`. **M3.** Drop `structuredContent` from the event tools, and point the tests at the wrapped text.
5. **M4, M7, M5, M6.** Change the id convention to `w-<story>-<task>-<attempt>` and use a cursor name that cannot be ambiguous. Require `AMB_ROLE`. Size reads with `num_ack_pending`. Use `consumers.update` on reopen.
6. **Tests.** Add the early-return latency check to P8, the late-completed superseding case, and polling instead of fixed sleeps in P9 and P12.

Repro scripts are in `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/A/`, `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/B/` and `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/C/`. My reruns are in `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/verify/`. The renderer left 5 minor writing-style warnings (passive voice and sentence length) on the HTML page.