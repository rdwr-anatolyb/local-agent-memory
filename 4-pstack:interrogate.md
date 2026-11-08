# Agent Memory Bus stage-1: round-5 re-review (job-13)

## Verdict

**Almost ready to approve as a stage-1 prototype. One small fix round remains.** Every round-4 closure in the code reproduces live. The heartbeat, subject and planner guards hold, and the real `amb mcp` heartbeat still passes the strict rules. One Medium defect must be fixed before approval.

1. **G1 (Medium).** `get_status {}` silently drops whole stories. The cut note does not name them, and README.md:76 says stories are "never dropped". Reviewers A and B found this independently and both reproduced it live.
2. **Cheap test asserts (Low, about one line each).** These are T1 to T4. They guard documented behaviour that new mutants can now break without any test failing. Examples: `lastSeq` removed from status lines, or a replay continue pointer that skips one event.

Everything else is a follow-up.

## Reviewers

All three reviewers ran on model `opus`, with no fable. All three finished and delivered final reports before this synthesis.

| Reviewer | Model | Angle | Findings |
|---|---|---|---|
| A | opus | correctness and regressions | 3 new and 1 nit |
| B | opus | trust/security guards and docs | 1 Medium and 3 nits |
| C | opus | test quality and mutation | 5 test gaps and 2 nits |

## Round-4 findings

| ID | Status | Evidence |
|---|---|---|
| A1 (status `{omitted}`) | **closed** | `re5-A/s2.ts` (live MCP). `cyr3` gives `done=true lastSeq=9`, 3 tasks `completed`, `artifact_count=20`. `ascii12` keeps all 12 tasks. `re5-A/s1.ts` ran 1500 random stories (1 to 600 tasks, Cyrillic, emoji, U+2028, lone surrogates). Result: `bad: 0, maxBytes: 49000`. correlation_id, done and lastSeq are preserved, and `tasks.length + tasks_omitted == n`. A sweep of 1 to 799 tasks had 0 over budget. |
| B-H (heartbeats cast) | **closed** | `re5-B/live.ts` and `live.log`. The stream reports `max_msg_size 4096` and rejects a 5000 B heartbeat. A heartbeat dated 2999 gives `stalled`. +4 s gives `started` and +6 s gives `stalled`. A foreign-subject heartbeat and a non-schema heartbeat are both ignored. A legit heartbeat from a real `amb mcp` process (spawned with the MCP SDK stdio client) passes. Stalled detection fires 2.2 s after the worker closes. |
| A3 (keep newest) | **closed** (one gap, G2) | `re5-A/s2.ts`. `get_decisions` returns `[d2,d3]` with the hint `from_seq=56 limit=2`. Pasting the hint returns exactly `[d0,d1]`. Replay without from_seq is also exact. |
| T-N13 | **closed** | The fake is `/bin/sh -c "sleep 20; :"` and the test asserts it is alive. The mutants `n13_no_name_check` and `r4_sd_prefix` are killed. |
| T-pgrep | **closed** (one gap) | The mutant `r4_no_pgrep_fallback` is killed. The `servesHome` filter after pgrep is unasserted (`pgrep_no_filter` survives). |
| B-N1 (null story/task) | **closed** | Live run gives `invalid event: task_id custom (task_assigned needs a story and a task)`. Mutant killed. |
| B-N2 (planner AMB_STORY) | **closed** | Live run gives `a planner bound to a story assigns only in its own story (S2)`. Mutant killed. |
| B-N3 (error echo) | **closed** | 8 hostile inputs, none echoed. Mutant killed. |
| B-N4 (truncation count) | **closed** | bus.ts:306. Mutant killed. |
| B-S (subject == subjectFor) | **closed** | 193 legitimate events with edge tokens (`_`, `-`, `__`, 64-char, `0`, null story/task) gave `invalid: none` (`re5-A/s3.ts`). A forged message on a mismatched subject is skipped. A forge on a matching subject still works, which is the accepted stage-1 no-auth limit (README.md:77). |
| README:73 | **closed** | The no-task `error` exception is documented. |
| D docs | **partial** | See D2 below. |
| Surviving-mutant asserts | **partial** | All round-4 survivors are now killed (`bh_no_subject`, `bh_no_schema`, `bn3_echo_message`, `a1_*`). The "97 mutants, 0 survive" claim holds only for the author's own set. Reviewer C's 44 new mutants of this round's code gave 28 killed and 16 survived (lead recount of `re5-C/new*.log`; C's summary said 43/29/14). |
| A2 docs | **closed** | Reviewer B checked tool names, arguments, limits and env vars side by side. |

## New findings, ranked

| ID | Severity | Location | Repro | Fix | Reviewer | Verified |
|---|---|---|---|---|---|---|
| G1 | **Medium** | src/mcp.ts:183-185 (with `wrapped` at mcp.ts:36-58), README.md:76, SKILL.md:27 | `statusLine` keeps each story at or under 49 KB, but `wrapped` then applies the same 49 KB to all stories together. So 2 ordinary stories fill one result. With 8 tasks of 20 artifacts per story, B saw `stories: BB` and the note `truncated: returned the first 1 of 4; pass correlation_id for one story`. A saw the same with 3 Cyrillic stories (`shown=SC`). The dropped ids are never named, so an all-stories planner or dashboard cannot find them. README.md:76 says "never dropped". | When there are k stories, shrink each line to LINES_BUDGET/k with the existing levels. At minimum, list the dropped correlation_ids in the note (at most 50 × 64 B). Fix README.md:76. | A, B | yes (live, both; lead checked B's log and README.md:76) |
| T1 | Low | src/mcp.ts:89; test/proofs.test.ts:335,358,393 | The mutant `sl_drop_lastSeq` survives with 132/132 passing. The planner-loop tests fall back to "next new event" and still finish. | Assert `typeof lastSeq === "number"` on status lines, including the cut form. Add a test where an event lands between two calls and `get_status({since_seq, wait_ms})` returns at once. | C | yes (mutant; lead confirmed in `re5-C/new1.log`) |
| T2 | Low | src/mcp.ts:51-52, 156-157 | The mutants `replay_always_head`, `replay_always_newest`, `hint_head_next_plus1` (silent skip of one event) and `hint_head_next_minus1` all survive. | Page a truncated replay with from_seq by following the pointer, and assert the union of pages has no gap or duplicate. Assert that replay without from_seq keeps the newest event. | C | yes (mutants) |
| T3 | Low | src/mcp.ts:82-87, 94; test/proofs.test.ts:1013-1037 | The mutants `sl_skip_summaries_step`, `sl_skip_compact_step` and `sl_compact_drops_state` survive. No fixture fits exactly at step 3 or step 4, and task states in the cut form are not asserted. | Add two fixtures sized for steps 3 and 4. Add `expect(b.tasks.every(t => t.state === "assigned"))`. | C | yes (mutants) |
| T4 | Low | src/bus.ts:22, 414-416 | The mutants `hb_skew_60s`, `hb_skew_0`, `hb_no_type_check` and `hb_no_task_null_check` survive. A schema-valid `finding` on a correct `amb.hb` subject is untested. | Add a heartbeat at now+30 s (must be ignored) and a non-heartbeat event_type on a correct hb subject. | C | yes (mutants) |
| G2 | Low | src/mcp.ts:53-55, 164; SKILL.md:25, README.md:76 | After `per_type_limit` the context is not a contiguous seq range. With d1, f1 to f4 and `per_type_limit=3`, the result is `[f3,f4]` with the hint `from_seq=69 limit=2`. The hint returns `[d1,f1]`, so f2 is never shown. | For get_task_context, list the omitted seqs, or hint `replay_events task_id=… from_seq=<oldest>` with no limit. | A (B traced the same by hand) | yes (live, A) |
| G3 | Low | src/event.ts:9-11, 67-68; src/identity.ts:67 | Token allows `_`, which is also the null placeholder in subjects. `replay_events correlation_id=_` returns null-story events too. A worker with `AMB_TASK=_` builds two identical filters, so NATS fails with `consumer subject filters cannot overlap` and `amb mcp` dies at startup. | Reserve `_`: reject it for story and task ids and for AMB_STORY and AMB_TASK. | A | yes (live; lead confirmed the regex and the filter code) |
| N2 | Nit | src/bus.ts:416 | The future check runs at read time. A +6 s heartbeat is rejected, then accepted 1.6 s later and keeps a dead task `started` until T plus the stall threshold. | Compare against the message store time (`m.info.timestampNanos`) plus the skew. | B | yes (live) |
| N3 | Nit | src/status.ts:41, src/bus.ts:402 | A self-consistent heartbeat on `amb.hb.amb.ZZ.ZZ.w-S1-T3-1` marks S1/T3 `started` in `get_status {}` but not in `{correlation_id:S1}`, so the two views disagree. No new forging power under no-auth. | Key heartbeats by story, task and agent. | B | yes (live) |
| N4 | Nit | src/mcp.ts:94-97 | The cut loop is O(n²): 5000 tasks take 1.5 s, and 50 stories of 600 tasks take 2.4 to 3.5 s CPU. Most of that work is then discarded by G1. | Binary search over n, or sum per-task bytes once. | A | yes (timed) |
| D2 | Nit | SKILL.md:8, 26, 50-51; mcp.ts:118, 120; README.md:75 | (1) "story and task are stamped, you cannot choose": semantic events accept `task_id`/`correlation_id` (bus.ts:169-170). (2) get_decisions "for a story or task" is project-wide without correlation_id. (3) The planner AMB_STORY rule is missing from the guide and the tool description. (4) "ignored if future": there is a 5 s skew. | Doc edits. | B | yes (side-by-side read) |
| T5 | Nit | test/proofs.test.ts (B-N3, A3, C2) | Tests compare exact Zod issue text and note prose. These are brittle across a Zod upgrade but are acceptable as contract text. | Match the code and path only. | C | yes (read) |

Also noted, not raised: the last-resort cut keeps the oldest tasks (`compact.slice(0, n)`), so the newest, most active tasks are the ones hidden (A).

## Dismissed claims

- **"97 mutants, 0 survive" means full coverage of this round.** It holds only for the author's set. 16 new mutants survive. Reviewer C's own summary miscounted (43/29/14); the lead recount from `re5-C/new*.log` is 44/28/16.
- **`fit_no_payload_guard` survival is a gap.** Dismissed. The mutant is equivalent because `statusLine` already keeps every line within budget (C).
- **`sl_budget_off_by_one` and `hint_limit_plus1` survivals.** These are boundary or prose only and low value. A's 1500-story sweep shows the byte budget is exact.
- **Forged events on a matching subject (B-S) and heartbeat-flood volume.** Accepted stage-1 no-auth limit, already documented in README.md:77.
- **Known accepted limits (N3-variant latency, no auth, approximate `cursor_status.gap`, cancel recovery via replay_events).** Not re-litigated, per the brief.

## Not checked

- About 80 older mutants from mutate6 were not rerun. C sampled 44, all new.
- A heartbeat flood across many subjects (AMB_HB has no `max_msgs`/`max_bytes`, only `max_age` 1 h), and the cost of draining heartbeats in `get_status {}`.
- A real MCP host's late-cancel behaviour.
- tsc was not rerun by reviewers. The orchestrator's clean result stands.

**Suite and process.** Three runs gave 132/132 in 44.6 s, 45.1 s and 45.2 s. One run under CPU stress (15 `yes` processes) gave 132/132 in 67.9 s, with no flakes. C killed the `yes` processes by PID and none remain. The final lead check of `pgrep -fl 'bin/nats-server|bun test|mutate|main.ts mcp'` is empty, and `pgrep -x yes` is empty. The worktree is unchanged: `git status` shows only the same untracked files. All mutants ran on copies under `re5-C/`.

## Recommended next actions

1. Fix G1. Budget `get_status {}` across stories (LINES_BUDGET/k per line using the existing shrink levels), or at least name the dropped correlation_ids in the note. Correct README.md:76 and SKILL.md:27. Add a test with 3 stories of about 30 KB each.
2. Add the cheap asserts T1 to T4: lastSeq present, task states in the cut form, fixtures for steps 3 and 4, replay pagination union, and heartbeat skew and type boundary tests.
3. Follow-up, not blocking: G2 (get_task_context hint), G3 (reserve `_`), N2 (store-time skew), N3 (heartbeat key), N4 (O(n²) cut), D2 doc edits, and the `pgrep_no_filter` assert.
4. Run a short round-6 check limited to G1 and the new asserts. A full panel is not needed.