# Agent Memory Bus stage-1: round-3 re-review (job-10)

## Verdict

**Not ready to approve as a stage-1 prototype yet.** Ten of the fifteen N findings are closed or behave as claimed (N5, N6, N7, N9, N10, N11, N12, N14, N15, and N8 for the old mutants). The second fix round introduced or exposed four defects that should be fixed before approval. Each fix is small.

1. **R2 (High).** The cursor can ACK an event the agent never saw.
2. **R1 (Medium).** A cancelled `read_events` can return the same event twice in the next result.
3. **B1 (Medium).** One malformed message can permanently break `get_status` and `replay_events`, and its text then reaches the model outside the envelope.
4. **R3 (Medium).** One large `task_result` replaces a whole story in `get_status` with `{omitted}`.

Everything else can be follow-up work: the N3 variant, R4, R5, R6, the surviving mutants, and the nits. tsc is clean and `bun test` passes 102/102. Reviewers A and C each ran it, and C's run took 31 s. The orchestrating session also re-ran it afterwards: 102 pass, 0 fail.

## Reviewers (all opus, no fable)

- Reviewer A: `opus`. Angle: correctness and regressions. 6 new findings.
- Reviewer B: `opus`. Angle: trust boundary and security, plus SKILL.md and README accuracy. 5 new findings.
- Reviewer C: `opus`. Angle: test quality, mutants, flakiness, token cost and docs. **It had not delivered its final report when this job had to return.** Its mutation run was still in progress. The results below come from its own log (`re3-C/mutate3.log`) and `run1.log`, which the lead read directly.

## N1-N15 status

| N | Status | Evidence |
|---|---|---|
| N1 | partial | Fixed for a single cancelled call. `re3-A/live1.ts`: call 1 times out and call 2 returns the batch. Call 3 prints `confirmed previous batch seq 1..1`. The shared-`held` NAK causes R1. |
| N2 | partial | Escaping is lossless and ASCII-only. `re3-A/pure.ts` round-trips astral characters, lone surrogates, U+2028, `＜／`, and a literal `<`. B confirmed that injected close tags give exactly one close line. Still broken: the cap boundary (R2) and `fit()` on status lines (R3). |
| N3 | partial | The reported case is fixed: with 1 event ready, `wait 10000` returns in 4 ms. A variant remains: nothing new is pending and a dead session's batch is still ack-pending. Then `wait_ms 0` costs 1004 ms, and an event published 1 s into an 8 s wait arrives at 8004 ms. Cause: `bus.ts:257` asks for `num_ack_pending` messages (`re3-A/live2.ts`). |
| N4 | partial | Docs only, as claimed. R6 is a related gap. |
| N5 | closed | Completed stays completed after a retried `task_assigned`. Attempt 2 gives `assigned` (`re3-A/pure.ts`). The mutant `n5_reset_attempt` is killed. |
| N6 | closed | With `AMB_STATUS_WINDOW_MS=1500`, a story whose assignment is older than the window shows `completed, done=true` (`re3-A/live3.ts`). |
| N7 | closed | Live forgery attempts are refused (`re3-B/live.ts`): a planner error about T1 or another story, an ad hoc error or task_result, and a case trick. Token rejects `"T1 "`, `""`, `null` and `"T1.>"`. AMB_FILTER bypasses are refused: `amb.ev.ambX.>`, `*`, `>`, uppercase, empty entries, another project, NBSP. |
| N8 | partial | Of the old mutants, 19 are killed and 2 are N/A because the code was deleted. 5 of C's new mutants **survive** (see T1). |
| N9 | closed | Two concurrent starts, then a result, give 0 beats. started/result/started gives exactly one timer. |
| N10 | closed | OLD is still listed after 50 stories that hold only findings. |
| N11 | closed | `correlation_id:"S17"` returns only S17. |
| N12 | closed | Rejected: a newline, `..`, `%2F`, uppercase, a third segment, 65 characters, fullwidth characters, `?x=1`. |
| N13 | partial | The `-sd` check works, with two gaps (R4). |
| N14 | closed | `replay own_scope from_seq=first_seq` returns exactly the batch. |
| N15 | closed | (a) to (d) fixed. `＜／` is now escaped. |

## New findings, by severity

| ID | Severity | Location | Repro | Fix | Reviewer | Verified |
|---|---|---|---|---|---|---|
| R2 | **High** | `src/bus.ts:284` vs `src/mcp.ts:40` | `bus.read` sums `escapedSize` without the newline byte, and `wrapped()` adds `+1` per line. With 10 events of 5000 bytes, read1 returns 9 items with `truncated=false` and a note saying `continue with from_seq=28`, but `read_events` has no `from_seq`. read2 then prints `confirmed previous batch seq 19..28`, so seq 28 is ACKed and never shown (`re3-A/live1.ts`). | Use one measure in both places, or skip the second cap for `read_events`. Never confirm an item that `wrapped` dropped. | A | **yes (lead reran: identical output)** |
| R1 | Medium | `src/mcp.ts:105-107`, `src/bus.ts:239-241`, no in-call dedupe at `bus.ts:292` | `releaseHeld()` NAKs the whole shared `held` list, and the cancelled handler keeps pulling until `wait_ms`. Sequence: the user presses Esc during a long-poll, then calls again. The new call's e1 is NAKed and refetched, so it is returned twice in one result: `B=["e1","e0","e1"]` (`re3-A/race.ts`). | Make `read()` own its messages and NAK only those. Pass `extra.signal` into `read()`. Restore a per-call seq dedupe. | A | **yes (lead reran: 4 of 4 runs)** |
| B1 | Medium | `src/bus.ts:344` (`AmbEvent.safeParse(m.json())` with no try) | Any local client publishes one non-JSON message on `amb.ev.<p>.<story>...`. After that, `get_status` (with or without a story) and `replay_events` fail for everyone for the whole retention period. The error text is `JSON Parse error: Unexpected identifier "IGNORE_ALL_PREVIOUS_INSTRUCTIONS"`, and it reaches the model outside the envelope. `read_events` skips the message correctly (`re3-B/live.ts`). | Put a try around `m.json()` in `replay()` and skip the message, as `read()` does. Keep payload text out of error messages. | B | yes (B live repro; lead confirmed by code read of the unguarded call) |
| R3 | Medium | `src/mcp.ts:25-29` with `trimStatus` at `mcp.ts:54-61` | A status line has no `payload`, so an oversized line becomes `{"omitted":"item of 56578 bytes..."}`. That drops `correlation_id`, `done`, `lastSeq` and every task state. A valid task_result with about 110 file artifacts is enough. The planner loop then cannot finish. The note text is also wrong for this case (`re3-A/live3.ts`, `re3-B/live.ts`). | In `trimStatus`, drop `result.payload.details` (keep outcome, attempt and artifact count). Cap `artifacts`. | A, B | yes (2 independent live repros; lead confirmed by code read) |
| R5 | Low | `src/identity.ts:46-52`, `bus.ts:407`, `bus.ts:208` | `AMB_STATUS_WINDOW_MS=9e15` passes validation, and then `toISOString()` throws `RangeError` on every `get_status` without a story. `AMB_HEARTBEAT_MS` above 2^31-1 is clamped to 1 ms (158 beats in 200 ms). `0.5` is accepted. | Require integer milliseconds in a range, for example 1000 to 2^31-1. | A | yes (pure) |
| R6 | Low | `src/bus.ts:185`, `SKILL.md:44` | After an MCP restart, a retried `task_started` with the same event_id is a duplicate, so no heartbeat starts and the worker shows as `stalled`: `duplicate true beats in 500ms: 0` (`re3-A/live2.ts`). | Start the heartbeat on a duplicate start when none is running. | A | yes |
| R4 | Low | `src/server.ts:57`, `server.ts:35-43` | (a) The `-sd` check is a substring match, so home `/x` matches a server whose store is `/x/jetstream-old/jetstream`. (b) Three parallel `ensureServer` calls all return `started`. A losing server overwrites the pid file with its own dead pid, so `stopServer` returns false (`re3-A/stop.ts`). | Match the argument exactly. If the recorded pid is dead, fall back to `pgrep -f "-sd <home>/jetstream"`. | A | yes |
| T1 | Low | `test/` | 5 of C's new mutants survive 102/102 (more were still running when this job returned). `n1_release_keeps_held`: releaseHeld does not clear `held`. `n2_no_gt`: `>` is not escaped. `n2_latin1_raw`: Latin-1 is not escaped. `budget_raw_json_bus`: the bus cap uses raw JSON, the same untested path as R2. `fit_no_note`: the omitted note is removed. | Add tests for each. R2's repro is the natural test for the bus cap. | C | yes (`re3-C/mutate3.log`) |
| B3 | Nit | `src/bus.ts:172`, `src/status.ts:56` | A `task_assigned` without `to_agent` is accepted and then ignored by status. `brief_ref` is not checked against the event's own story and task. | Require `to_agent`, and require brief_ref to equal `sdlc-flow://<story>/<task>`. | B | code read only |
| B4 | Nit | `src/mcp.ts:83`, `README.md:72` | The tool description says `brief_ref: URI` without the format. README says lifecycle events must name the publisher's own story and task, which ignores the no-task `error` exception. | Align the wording with SKILL.md:52. | B | yes (text vs code) |
| B5 | Nit | `src/bus.ts:333-357` | `replay` drains every matching message before it applies `limit`, so `limit:1` and the 24 h status window still read everything. A measured case: 300 stories, 50 picked, 42 ms. | Accept for stage 1, or bound the window scan. | B (A measured) | partly (A measured the cost) |

## Dismissed claims

- **The double ACK after R1 stalls later reads.** A measured 0-2 ms (`re3-A/race2.ts`).
- **`ps -o args=` truncation breaks the N13 check.** The args are 564 characters and not truncated.
- **AMB_FILTER entries such as `amb.ev.amb..>` or `>.x` escape the project.** NATS refuses them when the consumer is created, and they stay inside the prefix.
- **`<` in event text is ambiguous after escaping.** It becomes `\\u003c` and round-trips exactly.

## Not checked

- Reviewer C's final report never arrived. Missing parts are its full mutant table (the run was still in progress), the 3 normal runs plus 1 CPU-stress run (only one normal run is logged, 102 pass in 31 s), the token cost per event (English, Cyrillic or CJK, and code with many `<` and `>`), and C's own README and SKILL check. B covered the SKILL and README check against the tool schemas.
- How a real host behaves on cancel, and an abort that lands after the handler has returned.
- A "confirmed a..b" range that hides a failed ackAck in the middle.
- Raw forged heartbeats on `amb.hb.*`, which has no auth by design.
- **Processes.** At handback, reviewer C's mutation run was still going, with harness-child nats-servers on 127.0.0.1 and temp stores under `scratchpad/re3-C/`. The orchestrating session stopped that run and every process under that path. `pgrep -fl nats-server`, `bun test` and `mutate3` then found nothing. The worktree source was not touched: the mutants ran on copies, and the suite still passes 102/102 afterwards.

## Recommended next actions

1. Fix R2. Use one size measure, and add `re3-A/live1.ts` CAP as a test. This also kills `budget_raw_json_bus`.
2. Fix R1. Use per-call held messages, a signal-aware `read()`, and a seq dedupe. Add `re3-A/race.ts` as a test.
3. Fix B1. Guard `m.json()` in `replay()`, and add a poison-message test for `get_status` and `replay_events`.
4. Fix R3. Trim status result details, and add a test with a 110-artifact result.
5. Follow-up: the N3 ack-pending variant, R5 env ranges, R6 heartbeat on a duplicate start, R4 pid handling, tests for the 5 surviving mutants, B3, B4 and B5. Then rerun C's flakiness and token-cost checks.