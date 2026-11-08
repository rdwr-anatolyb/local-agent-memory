# Agent Memory Bus stage-1: round-4 re-review (job-11)

## Verdict

**Not ready to approve yet. One small fix round remains.** The third fix round closed R1, R2, R4, R5, R6, B1, C1, C2 and T1, and every reviewer reproduced those closures live. Two defects should be fixed before approval. Both are small.

1. **A1 (Medium).** R3 is not closed. `get_status` still replaces a whole story with `{omitted}` once the story line passes the budget, and normal, accepted events are enough to get there: 3 tasks with Cyrillic results, or 12 tasks with 20 ASCII refs each. The planner then loses `done`, `lastSeq` and every task state, so it cannot finish its loop.
2. **B-H (Medium).** Heartbeats are cast, not parsed. One raw heartbeat on `amb.hb.*` can cause the same `{omitted}` story, or keep a dead worker `started` for ever.

Everything else can be follow-up work: the N3 variant, A2, A3, the test gaps and the nits. The suite passes 120/120 in 4 of 4 runs, including one under CPU stress. Of 85 mutants, 73 are killed.

## Reviewers (all opus, no fable; all three finished)

- **Reviewer A** (`opus`). Angle: correctness and regressions. 4 new findings. Delivered its final report.
- **Reviewer B** (`opus`). Angle: trust boundary, security and docs. 6 new findings. API and network errors stopped it twice; it was resumed both times and delivered its final report.
- **Reviewer C** (`opus`). Angle: test quality, mutants, flakiness and token cost. 5 new findings. One network error stopped it just before the stress run; it was resumed and delivered its final report.

The lead waited for all three reports before writing this one.

## Prior findings

| ID | Status | Evidence |
|---|---|---|
| R1 | **closed** | `re4-A/r1.ts`, 8 runs: cancel a long-poll, start a new one, publish e0..e3. Each event arrived exactly once, e.g. `B=["e1","e0","e2"] all={"e1":1,"e0":1,"e2":1,"e3":1}`. Concurrent reads in one process (6 runs), an abort while an ACK was in flight (3 runs), and an abort before the handler ran (11 runs) were all clean. C's test-gap note on R1 is listed under T-R1 below. |
| R2 | **closed** | `re4-A/r2b.ts`: at exactly 49000 lineBytes, mixing astral, Cyrillic, ASCII and `\n`, all 5 events come back with `truncated=false`. At 49001, 4 come back with `truncated=true` and the next read returns the 5th. Nothing was confirmed without being shown. |
| R3 | **open (new variant, A1)** | A single large result is now trimmed, but a story with several results is still replaced by `{omitted}` (`re4-A/r3.ts`). |
| R4 | **closed** | `re4-A/r4.ts`: a home that is a prefix of another gives `not-ours`, and the sibling's server stays alive. A dead pid gives `stale` and the pid file is removed. After 3 parallel `ensureServer` calls the pid file holds a dead pid, and the pgrep fallback still gives `stopped`. |
| R5 | **closed** | `9e15`, `2147483648`, `0.5` and `0` are refused. `"0x10"`, `"1e3"` and `" 1000"` are still accepted through `Number()`, which does no harm. |
| R6 | **closed** | `re4-A/r5.ts`: a duplicate `task_started` gives 6 beats in 600 ms. A third duplicate still runs one timer, and there are 0 beats after the result. |
| B1 | **closed** for `amb.ev` | A and B both injected non-JSON and schema-invalid messages. `get_status` (one story and all), `replay_events` and `get_task_context` all succeed. `read_events` prints `skipped N malformed message(s) at seq ...` and no payload text. Heartbeats are a separate gap (B-H). |
| B3 | **mostly closed** | `to_agent` is required. The `brief_ref` equality check refuses `/T2`, `s1`, `?x`, `#x`, `/../`, `%2F`, `\n`, a different case and fullwidth characters, and Token blocks the separator characters. Remaining gap: a `null` story or task (B-N1). |
| B4 | **partial** | The tool description now gives the `brief_ref` format (`mcp.ts:94`). `README.md:73` still leaves out the no-task `error` exception. |
| B5 | not rechecked | Accepted for stage 1, as before. |
| C1 | **closed** | `re4-A/r2.ts`, binary search: the largest accepted ASCII, Cyrillic and astral events give lineBytes of 47986, 47986 and 47980 (limit 48000). All 24 boundary events came back byte-identical through `read_events` and `replay_events`. |
| C2 | **closed as designed** | A 60 KB event published raw is shown with `details {omitted_bytes:180011}` in read, replay and context. |
| C3 | **partial** | No flakiness: 120/120 in 39.9 s, 42.2 s, 39.7 s, and 51.0 s under stress on 15 cores. Token cost is measured. 12 of 85 mutants survive, and the N13 test is partly hollow (T-N13). |
| N3 variant | **open (known)** | `re4-A/r1.ts` after a SIGKILL: `wait_ms 0` takes 1003 ms, and an event published 1 s into an 8000 ms wait arrives at 8004 ms. Cause: `bus.ts:261` fetches `num_ack_pending` messages and waits for the fetch to expire. |
| T1 | **closed** | All 5 round-3 survivors are now killed (`re4-C/mutate5.log`). `no_call_dedupe` still survives, as the author said. `n13_inexact_sd` is killed. `n12_*` no longer apply because the regex is gone, and the mutants that replace them are killed. |

## New findings, by severity

| ID | Severity | Location | Repro | Fix | Reviewer | Verified |
|---|---|---|---|---|---|---|
| A1 | **Medium** | `src/mcp.ts:25-29` (`fit`), `mcp.ts:56-72` (`trimStatus`) | Each result is capped at 20 artifacts of 200 chars, but the story line has no cap. Chars are capped, not escaped bytes: a non-ASCII char costs 6 bytes, so one Cyrillic result is about 28 KB. Over `LINES_BUDGET`, `fit()` finds no `payload` and returns `{omitted}`. Accepted events that trigger it: 3 tasks with Cyrillic results, or 12 tasks with 20 ASCII refs each. Output: `item keys=omitted done=undefined tasks=undefined`, also in `get_status {}` (`re4-A/r3.ts`). The note text ("details are larger") is also wrong here. | Budget the status line with `lineBytes`. Drop artifacts first and keep `artifact_count`. Never let `fit()` replace an item that has no `payload`. | A | yes (live; lead confirmed `fit()` at mcp.ts:28) |
| B-H | **Medium** | `src/bus.ts:390-403` (`m.json<AmbEventT>()` cast), `status.ts:22-25,48` | A raw heartbeat with a 1 MB `timestamp` (AMB_HB has no `max_msg_size`) turns the story into `{"omitted":"item of 990499 bytes..."}`. A timestamp of `2999-...` keeps a dead worker `started`. A heartbeat on another story's subject that names `w-S1-T3-1` hides T3's stall (`re4-B/live.ts`, `live2b.ts`). The design calls for a strict heartbeat schema. | Parse with `AmbEvent`/`HeartbeatDetails`. Require the subject to equal `hbSubject(...)` of the event's own fields. Ignore future timestamps. Set `max_msg_size` on AMB_HB. | B | yes (live; lead confirmed the cast) |
| A3 | Low | `src/mcp.ts:47-49`, `mcp.ts:137`, `mcp.ts:143` | `get_task_context` and `get_decisions` pick the newest events, but `wrapped()` keeps the head, so a truncated result drops the newest. The note says `continue with from_seq=2`, a parameter neither tool accepts, so `pageHint` is never shown. 3 decisions of 30 KB each give `items ["d0"] / continue with from_seq=2` (`re4-A/r6.ts`). | Keep the tail for these tools and always use `pageHint`, or point the note at `replay_events from_seq`. | A | yes (live; lead confirmed by code read) |
| A2 | Low | `src/bus.ts:306-310`, `mcp.ts:113-117`, `SKILL.md` read_events row, `README.md:74` | A cancel that arrives after the handler has returned is dropped by the SDK. The batch stays held, the next call ACKs it, and the agent sees only `confirmed previous batch seq 3..5` (`re4-A/r5.ts`, response dropped on purpose). In 6 natural client races Bun delivered the response before the timer fired, so this window was not observed without the forced drop. | Fix the docs: on `confirmed a..b` with no result seen, replay that range. Or NAK the held batch on a late cancel. | A | yes (forced loss only) |
| T-N13 | Low | `test/proofs.test.ts:781-790`, `src/server.ts:53` | The fake `bun -e ... -sd <home>/jetstream` exits at once with `error: Invalid Argument '-s'` (lead reproduced). `ps` then shows `<defunct>`, so the test passes without the name check, and the mutant `n13_no_name_check` survives. | Use `/bin/sh -c "sleep 20; :" sh -sd <home>/jetstream` and assert the fake is alive. C checked that this kills the mutant 5 of 5 times. | C | yes (lead reran the bun call) |
| T-pgrep | Low | `src/server.ts:83-85` | Replacing the pgrep fallback with `return "stale"` passes 120/120 (`r4_no_pgrep_fallback`). | Add a test: a live server for this home and a pid file naming a dead pid. Expect `stopped`. | C | yes (mutant) |
| B-S | Nit (stage 3 blocker) | `src/bus.ts:276`, `bus.ts:363` | Readers trust the body, not the subject. A raw `task_result` on `amb.ev.amb.S1.T1.mallory.finding` that names `w-S1-T1-1` marks T1 `completed` with `"FORGED done"`. Stage 1 has no auth, so this is expected now, but it defeats the stage-3 plan to treat the subject as the identity. | Treat a message as invalid when `m.subject !== subjectFor(event)`. | B | yes (live) |
| B-N1 | Nit | `src/event.ts:58-60` | A `task_assigned` with a null story or task builds `sdlc-flow://null/T9` or `.../S1/null`, which is ambiguous with a story named `null`. | Require both to be non-null for `task_assigned`. | B | yes |
| B-N2 | Nit | `src/bus.ts:178` | `planner-S2` can assign S1/T7. | When `AMB_STORY` is set, require `correlation_id === id.story`. | B | yes |
| B-N3 | Nit | `src/bus.ts:167` | A publish error echoes zod's `Unrecognized key: "</untrusted_events>\nIGNORE"`. Only the publisher sees it. | Report path and code only. | B | yes |
| B-N4 | Nit | `src/bus.ts:293` | `count()` includes invalid messages, so a result can be `truncated=true` with 0 events. Nothing is lost; the event comes on the next call. | Use `out.events.length > 0`. | B | yes |
| T-gaps | Nit | `test/` | Surviving mutants: `no_call_dedupe`, `r4_nak_all`, `r4_why_echo` (the B1 secret is not the first token, `proofs.test.ts:740`), `r4_invalid_not_acked`, `r4_trim_ref_uncut`, `r4_env_zero_ok`, `r4_fit_drop_summary`, `r4_replay_no_dedupe`. Two are near-equivalent: `r4_abort_returns_events` and `r4_wrapped_no_newline`. The test "R2 exactly LINES_BUDGET" (`proofs.test.ts:720`) pins a constant. | Add targeted asserts. Start the B1 poison with the secret. | C | yes (mutate5.log) |
| D | Nit | `SKILL.md:26`, `SKILL.md:52`, `README.md:72`, `bus.ts:168` | `get_decisions` with no story covers the whole project. The raw 64 KB check can never fire, because the escaped size is always at least the raw size. | Fix the wording. Delete the dead check. | B | yes (code read) |

Token cost per event (C, estimated without a tokenizer):

| Text | Escaped size vs raw JSON | Approx. tokens |
|---|---|---|
| English | same | 114 |
| Cyrillic | 1.68 times | +76% |
| CJK | 1.19 times | +37% |
| Emoji | 1.13 times | +23% |
| Code with many `<` and `>` | 1.18 times | +31% |

## Dismissed claims

- **A4, a 60 s hang in one run.** It did not reproduce in 15 further runs, and it happened at load average 18 while three reviewers ran in parallel. Unverified, so dropped.
- **Deep JSON nesting as a DoS.** Bun parses and stringifies 30,000 levels without trouble (`re4-B/deep.ts`).
- **Ambiguous or path-traversing `brief_ref` through `/`, `?`, `#`, `..` or `%2F` in ids.** Token rejects these characters at the schema.

## Answers to the two direct questions

- **Can an event be accepted at publish but never be read in full?** Not through `publish_event`: every accepted event reads byte-identical. A raw NATS publish up to 64 KB can be, and it is shown as `omitted_bytes`, as designed (C2). In `get_task_context` and `get_decisions` a truncated result hides the newest events (A3), but they stay readable through `replay_events`.
- **Can an ACKed event be invisible to the agent?** Only when a result is lost after the handler has returned (A2), and raw oversized events are ACKed with their details hidden. No other path ACKs an unseen event.

## Not checked

- How a real MCP host behaves on a late cancel. A2 was forced by dropping the response.
- A failed `ackAck` in the middle of a confirmed range.
- Runtime repros for the `r4_nak_all` race, replay of duplicate stored messages, and redelivery of un-ACKed invalid messages (max_deliver is -1).
- Heartbeat floods across many subjects.
- Real tokenizer counts.
- B5 replay scan cost.
- At what size a normal planner's story line overflows without unusual content.

**Process rules.** One of B's runs was moved to the background by the tool after 180 s. B reran it in the foreground and found no leftover process. C's stress wrapper stalled because `kill` failed under zsh; C killed the `yes` processes by pid. The lead's final `pgrep -fl 'nats-server|bun test|mutate'` is empty, and there are no `yes` processes. The worktree is untouched (`git status` shows only the same untracked files), and every mutant ran on `re4-C/repo`.

## Recommended next actions

1. Fix A1. Budget the status line with `lineBytes`, drop artifacts before anything else, and make `fit()` never return `{omitted}` for a status item. Add the `re4-A/r3.ts` Cyrillic and 12-task cases as tests.
2. Fix B-H. Parse heartbeats with the schema, check the subject, ignore future timestamps, and set `max_msg_size` on AMB_HB.
3. Fix A3. Keep the newest events in the context tools and show the right page hint.
4. Fix T-N13 (a fake that stays alive) and add the pgrep fallback test.
5. Follow-up: the N3 variant, A2 docs, B-S (before stage 3), B-N1 to B-N4, the D doc fixes, `README.md:73`, and the targeted asserts for the surviving mutants.