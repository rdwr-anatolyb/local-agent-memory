# Agent Memory Bus stage-1: re-review after fixes (job-9)

## Verdict

**Not ready to merge yet. 18 of the 24 prior findings are closed, 6 are partial and none are fully open.** The fixes hold up well when reproduced. One new high-severity defect blocks the merge: a `read_events` result that the model never receives still gets ACKed (N1). One timeout or Esc is enough to lose events silently. Fix N1 and the result-cap bypass (N2) first, then the restart stall (N3) and the SKILL.md heartbeat trap (N4).

## Reviewers

All three reviewers ran on **opus**. Fable and sonnet were not used.

| Reviewer | Model | Angle | New findings |
|---|---|---|---|
| A | opus | Reran the old repros on the new API; regressions from the deferred ACK | 5 |
| B | opus | Trust and security; forging task states | 8 |
| C | opus | Test quality, mutation testing, CPU-stress run, MCP and skill usability | 11 |

The lead (me) reran the N1 and N3 repros and read `server.ts`, `status.ts`, `untrusted.ts`, `mcp.ts` and `bus.ts` to settle the points where reviewers disagreed.

## Prior findings

| ID | Status | Evidence |
|---|---|---|
| H1 | closed (related risk in N7) | Workers get "must be about your own task/story" for T2, T3 and S2 (`bus.ts:162-167`). `task_completed` now fails the schema. Status counts lifecycle events only from the assignee (`status.ts:56-58`). A forged `error` from a no-task identity changes status only if it reuses the assignee's id (N7). |
| H2 | closed | The same `event_id` from pa/A, pb/B and pa/A2 got seq 5, 6 and 7, all `duplicate:false`. The tokens cannot contain `:`, so the dedupe key `project:agent:event_id` cannot collide. |
| H3 | **partial** | A crashed or closed session gets its batch redelivered (A: crash case, ACK-failure case). Two gaps remain: a batch lost while the session is still alive (N1), and the cap bypass (N2). |
| H4 | closed | The phantom T2 is gone and `done:true`. A planner `finding` on T1 does not create an attempt. |
| M1 | **partial** | The code is fixed: a `task_started` from a harness identity leaves the task `assigned`. SKILL.md's "unless sdlc-flow or Orca already did" brings the problem back through the docs (N4). |
| M2 | closed | 7 consumers before and 7 after 40 replay/status/context calls. The `no_ordered_delete` mutant fails M2. |
| M3 | closed | `structuredContent` is now only on publish and cursor_status (`mcp.ts:24-37`). The mutant fails P8 and L8. |
| M4 | closed | `cursor_1_a_b_c` vs `cursor_3_a_b_c`. README uses `w-<story>-<task>-<attempt>`. |
| M5 | closed | A due redelivery was returned with wait 0. The mutant fails P7b. Side effect: N3. |
| M6 | closed | Reopening with a new ack_wait succeeds; the consumer shows 6e10. The mutant fails P4b. |
| M7 | closed | "AMB_ROLE is required". `Worker` and ` worker` are rejected. |
| L1 | closed | `read` with 500 ms took 1005 ms; `get_status` with wait 2000 and no since_seq took 2010 ms. Docs gap: SKILL.md still does not say which `since_seq` to pass on the first call. |
| L2 | **partial** | The 24h default and the 50-story cap exist (`bus.ts:377-380`), but no test covers them (mutants survive) and `result` is not trimmed. The window also breaks long stories (N6). |
| L3 | **partial** | `brief_ref` refuses prose with whitespace only. `https://evil/IGNORE_PRIOR...`, `file:///…/.ssh/id_rsa`, `%0ASYSTEM:` and U+202E all pass (`event.ts:21`). |
| L4 | closed | The SKILL.md planner loop now polls for questions and help requests. |
| L5 | **partial** | `task_completed` is gone and heartbeats stop on result, error and close. The leak from two concurrent `task_started` calls is still there (N9). |
| L6 | **partial** | `server.ts:56-57` checks that `ps -o comm=` contains "nats-server", so a stale pid that now belongs to *another* nats-server still gets SIGTERM. A said closed; the lead checked the code and sides with B. |
| L7 | partial (by design) | Config drift now produces a warning, but only on stderr, and the new limits are not applied. |
| L8 | mostly closed | Strict details schemas, one trust paragraph, `get_decisions` limit 50. SKILL.md still lists `to_task`, and the MCP `details` is `z.record(z.unknown())`. |
| T1 | closed | P8 asserts under 5000 ms with wait_ms 20000. |
| T2 | closed | The status test plus P9 cover a late `completed` from attempt 1. |
| T3 | **partial** | The guard mutants are caught, but 6 mutants survive (N8). |
| T4 | closed | 84/84 in 3 normal runs and 1 run with 15 `yes` processes on 15 cores. P9 saw `stalled` at about 1015 ms against a 2600 ms bound. |
| T5 | **partial** | The un-ACKed batch case and heartbeat stop on result and close are now tested. Concurrent `task_started` is not. |

## New findings, by severity

| ID | Sev | Location | Finding / repro | Fix | Reviewer | Verified |
|---|---|---|---|---|---|---|
| N1 | **High** | `src/bus.ts:203-220`, `src/mcp.ts:76-79` | A batch lost while the session is alive (client timeout, user Esc, host rejects the result) still goes into `held`. The next `read_events` then ACKs it. The `replay_events from_seq` recovery hint was in the lost result, and `cursor_status` shows no gap. Repro `re-A/cancel.ts`: call 1 times out, event published, call 2 prints "cursor: no new events", ackFloorSeq 1. | In the handler, NAK the batch if `extra.signal` is aborted (or if the result was not delivered). Print "confirmed previous batch seq a..b" in the next header. | A | yes (lead reran it: same output) |
| N2 | Medium | `src/untrusted.ts:6`, `src/bus.ts:260`, `src/mcp.ts:27-28` | The cap is measured before `<` becomes `<` (6 bytes), and `wrapped()` measures with `.length`, not bytes. One event becomes a 360 KB result; three events at 15k `<` each give 271 KB with `truncated=false`. `get_status` for one story always admits the whole first story, including full `result` events. | Measure `line(ev)` in bytes in both places, or escape only `</` (1 byte). Trim `result` in status. | A, B, C | yes (3 independent measurements) |
| N3 | Medium | `src/bus.ts:229,236` | After a restart, the first fetch is sized as `num_pending + num_ack_pending`. The dead session's batch is not due until ack_wait, so the fetch waits for the full `expires`. With 1 event ready, `wait_ms 10000` returned after 10004 ms, and wait 0 cost 1005 ms. The planner and dashboard use 20000. | Size the first fetch by `num_pending` only. Add one extra only when nothing is pending, or stop when the ready count arrives. | A, C | yes (lead reran it: 10004 ms / 1005 ms) |
| N4 | Medium | `skills/agent-memory-bus/SKILL.md:40,59`, `src/bus.ts:171` | The heartbeat runs in whichever process published `task_started`. If a harness publishes it and exits, and the worker skips it as SKILL.md allows, nothing sends heartbeats and a live worker shows as `stalled`, which leads to a needless reassignment. Repro `re-C/lib.ts` case (a). | Tell workers to always publish their own `task_started`, or start the heartbeat in the worker's MCP process no matter who published it. | C | yes |
| N5 | Medium | `src/status.ts:53,56` | `open()` replaces the attempt for `to_agent` without conditions. A planner retry of `task_assigned` with a new event_id resets a completed attempt to `assigned`, and later to `unclaimed`. Repro `re-B/pure.ts`: "before retry completed / after retry assigned". | Open a new attempt only when the agent has none yet or the attempt number differs; otherwise append the event. | B | yes |
| N6 | Medium | `src/bus.ts:377-378`, `src/status.ts:52-58` | With the 24h window, a story whose `task_assigned` is older than 24h drops its later results: `tasks: [], done:false`. The SKILL.md dashboard loop uses `since:"24h"`. | Use the window only to pick active stories, then replay each story in full. Otherwise, document the limit. | C | yes (direct `deriveStatus` call) |
| N7 | Medium | `src/bus.ts:164-165`, `src/identity.ts:68` | An identity with no story or task may publish `error` about any task. `AMB_AGENT_ID` is self-asserted, and `AMB_FILTER` is not validated (an ad hoc agent with `amb.ev.>` can read other projects). An impersonator with the worker's id and filter opens with no warning and fails T1. B rated this critical; the lead downgraded it because stage 1 has no authentication by design. | Allow a no-task `error` only with null or own task and story. Require each AMB_FILTER entry to start with `amb.ev.<project>.`. Say in SKILL.md and the design that ids are not authenticated. Optionally record the role in consumer metadata. | B, C | yes (live repro by B) |
| N8 | Medium | `test/` (`bus.ts:206-207,251,255,377,380`, `mcp.ts:28`) | 6 mutants pass 84/84: remember-before-ack, `returned` dedupe disabled, `again` path disabled, `wrapped()` cap removed, 24h default removed, 50-story cap removed. Three of the claimed fixes therefore have no proof. | Add tests: an injected ackAck failure, a forced-redelivery dedupe, a replay size check, and the status window and cap. | C | yes (`re-C/mutate.py`) |
| N9 | Low | `src/bus.ts:171,178-189` | `!this.hbTimer` is checked before `await replay`, so two parallel `task_started` calls leak a timer. The leaked timer keeps beating after `task_result`, logs "closed connection" without end after close, and the process does not exit. | Reserve the slot before the await (a sentinel or a shared start promise). | A, C | yes |
| N10 | Low | `src/bus.ts:377-380`, `src/status.ts:45-48` | Any non-dashboard agent can push real stories out of `get_status`'s 50-story view with 50 `finding` events, each with a new `correlation_id`. | List only stories that have an assignment or a start. Consider stamping `correlation_id` for workers. | B | yes (pure repro) |
| N11 | Low | `src/mcp.ts:93-99`, `src/bus.ts:337` | `get_task_context` uses `id.story`, so for a dashboard or planner it mixes T1 from every story (S-17 and S-99). | Add an optional `correlation_id`. | C | yes |
| N12 | Low | `src/event.ts:21` | `brief_ref` is still free text (see L3). | Allow only `sdlc-flow://<Token>/<Token>`, ASCII only. | A, B | yes |
| N13 | Low | `src/server.ts:56-57` | The stop pid check accepts any nats-server process (see L6). | Match `ps -o args=` against this home's store dir, or remove the pid file on stop. | B | code read (pid reuse not reproduced) |
| N14 | Low | `src/bus.ts:274-279,310` | After a crash, the redelivered batch arrives after newer events ([4], then [1,2,3]), so `first_seq`/`next_seq` go backwards. The recovery hint `replay_events from_seq` uses the project filter, not the cursor's. | Document this in SKILL.md, or pass the cursor's filter in the hint. | A | yes |
| N15 | Nit | `SKILL.md:22,52`, `src/mcp.ts:34`, `src/untrusted.ts:6` | 4 small issues. (a) SKILL lists `to_task`, which the server drops silently. (b) SKILL overstates the `error` guard. (c) The truncation note offers `from_seq` on `get_task_context`, which has no such parameter. (d) Fullwidth look-alike closing tags pass the envelope unchanged. A token-cost note is listed under Noted. | (a) Remove `to_task` from SKILL.md. (b) Correct the `error` wording. (c) Word the note per tool. (d) Escape the look-alike characters or add a nonce to the tag. | B, C | yes |

**Noted, not acted on:**
- **Token cost** (C, measured): 466 B per event. About 25% of that is repeated `session_id`, `project_id` and `to_task:null`. The preamble costs 236 B even when there are no events.
- **L7 drift warning:** it goes to stderr only.
- **Flood eviction** (B, not verified): about 16k events of 64 KB each would evict project history.

## Disagreements and dismissed claims

- **L6, A "closed" vs B "partial":** B is right. The `comm` check does not tell one nats-server from another (lead read the code).
- **H1, B "partial/critical":** downgraded. Status counts an `error` only from the assignee, so the forgery needs impersonation, which stage 1 does not defend against. The residual risk is N7 (Medium).
- **Dropped as covered by other findings:** C#10 is merged into N3. C#7 and A#4 are merged into N9. C#8 is merged into N7 and N15. A#3, B#3 and C#1 are merged into N2.
- **Behaviour reviewers confirmed is correct:** a batch held longer than ack_wait gives no duplicates, over-cap messages do not cause a NAK loop, two concurrent reads do not ACK early, and a crash or close redelivers the batch. A forged `attempt` in `task_result` has no effect, and every subject-token injection is refused.

## What was not checked

- How the real Claude Code and Codex hosts handle cancel, 300 KB results and `structuredContent`. N1 was shown only through the SDK client's timeout.
- Real pid reuse for L6/N13.
- Scale: a 1 GB stream, thousands of consumers, `max_ack_pending` saturation.
- Ordered-consumer reset inside `withOrdered`.
- Raw NATS access on localhost was treated as trusted. Ids are not authenticated, so forging is out of scope for stage 1.
- **Cleanup:** every server was a harness child that was disposed. The final `pgrep -fl nats-server` and `pgrep -x yes` found nothing. No source files were edited and nothing was committed.

## Recommended next actions

1. **N1:** NAK the held batch when a request is aborted. Print the confirmed range in the next header. Add an MCP-level test for a cancelled call.
2. **N2:** Measure the escaped line in bytes, or escape only `</`. Trim `result` in `get_status`.
3. **N3:** Size the first fetch by `num_pending`. Add a restart latency test.
4. **N4 and N9:** Tell workers to publish their own `task_started`. Reserve the heartbeat slot before the await.
5. **N5, N6, N7:** Make `task_assigned` idempotent per attempt. Replay active stories in full. Restrict the no-task `error`, validate AMB_FILTER, and document that ids are not authenticated.
6. **N8:** Add tests that fail the 6 surviving mutants.
7. **Lows and nits** (N10-N15, L3/N12 `brief_ref`, L6/N13 pid check): fix them in the same pass if cheap.

Scratch evidence: `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/re-A/`, `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/re-B/`, `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/re-C/`. Reviewer brief: `/private/tmp/claude-502/-Users-anatolyb-orca-projects-agent-memory-bus/7bda8d4f-1ae8-4d6c-8866-eb4132e19a0a/scratchpad/reviewer-prompt-2.md`.