# Round-6 interrogate: Agent Memory Bus stage-1 prototype (job-14)

Note on this document: the two reviewers reported separately. Reviewer A finished first and its report came back with the review agent. Reviewer B finished afterwards, so its results were merged here by the orchestrating session. The HTML page was built before B finished and covers reviewer A only. Both reviewers ran on `opus`. No `fable` was used.

## Verdict

**Ready to approve as a stage-1 prototype, after two small documentation fixes (D3, D4).** No critical findings. Reviewer A confirmed G1 live. Reviewer B found no warning-level defects: its regression hunt on `heartbeatFrom` and G3 was clean, 21 of 25 independent mutants were killed, and the full suite passed under CPU stress. Everything else is a nit or a follow-up.

Orchestrator checks: `tsc --noEmit` clean and `bun test` 152 pass, 0 fail (46 s) after the round-5 fixes. At the end of this round, `pgrep -fl 'bin/nats-server|bun test|mutate|main.ts mcp|^yes'` printed nothing.

## Reviewers

| Reviewer | Model | Status |
|---|---|---|
| A | opus | Finished. Live G1 verification, statusLine budget exactness, docs. 6 findings. |
| B | opus | Finished (after the first hand-back). Regression hunt, 25 mutants, CPU-stress run. 4 nits. |

## Round-5 findings

| ID | Status | Evidence |
|---|---|---|
| G1 | **closed** for picked stories | A, `re6-A/live.ts`, k = 1, 3, 10, 50 and 58 stories mixing ASCII, Cyrillic, emoji and a 600-task story. Every picked story has `correlation_id`, `done`, a numeric `lastSeq`, and task states or `tasks_omitted`. For every story, `tasks.length + tasks_omitted` equals the full task count. `done` and `lastSeq` match `get_status {correlation_id}`. Largest result 49,049 bytes (k=1); k=50 is 29,331 bytes. Stories beyond 50 are dropped without a signal (D3). B checked the budget arithmetic: the minimum share is 980 B and the shares sum to at most LINES_BUDGET. |
| T1 | **closed** | B's mutant sweep: `sl_drop_lastSeq` and the status tests are killed (full table below). |
| T2 | **closed** | Replay paging mutants were in the author's set, which A's and B's runs did not contradict. B did not re-run those four mutants. |
| T3 | **closed** | B: `sl_skip_level0` to `sl_skip_level3`, `sl_level_lt`, `sl_cut_extra`, `sl_cut_no_omitted` and `sl_cut_no_loop` are all killed. One cut-boundary mutant survives (N-B3). |
| T4 | **mostly closed** | B: `hb_always_none`, `hb_no_type`, `hb_no_task_null`, `hb_no_subject`, `hb_no_skew_bound` and `hb_skew_0` are killed. Three boundary mutants survive (N-B1, N-B2). |
| pgrep_no_filter | **closed** | In the author's set and not contradicted. B did not re-run it. |
| G3 | **closed** | B: the four `g3_*` mutants are killed. `publish_event` with `correlation_id:_` or `task_id:_` gives `invalid event: correlation_id custom ("_" is reserved for none)`. AMB_STORY=_ and AMB_TASK=_ are refused. Queries with `_` still work. |
| D2 | **partial** | SKILL.md:26 still says `get_decisions` is "for a story or task" although without a `correlation_id` it is project-wide. README.md:75 says "ignored if they claim a future time" without the 5 s skew. |

## New findings, ranked

| ID | Severity | Location | Repro | Fix | Reviewer | Verified |
|---|---|---|---|---|---|---|
| D3 | Warning (docs) | README.md:76, skills/agent-memory-bus/SKILL.md:27, src/bus.ts:457 | With 58 active stories, `get_status {}` returns 50 with no header or note, and the 8 oldest disappear. Both docs still say that a story left out "is named in the note", but that branch was deleted. B independently found the same wording problem (N-B4). | Return the active count from `getStatus` and add a header such as `showing 50 of 58 stories with the most recent task activity; pass correlation_id or a narrower since`. At least delete the clause from both docs. | A, B | yes (live, A; B read the code and docs) |
| D4 | Warning (docs) | bus.ts:456-457, README:76, SKILL.md:27, `get_status` tool description | Stories are ranked by the seq of their latest `task_assigned` or lifecycle event, newest first, and the top 50 are kept. Docs say only "up to 50". | Add one sentence: "the 50 with the most recent task activity, most recent first". | A | yes (live) |
| F1 | Follow-up | src/mcp.ts:104-106 | Unused share is not handed on. At k=10 the result is 22,232 bytes, yet the 600-task story shows only 25 tasks. At k=50 about 20 KB goes unused. | A second pass that splits unused bytes among the stories that were cut. | A | yes (live) |
| F2 | Nit | src/mcp.ts:93-95 | The cut loop is O(n²) (round-5 N4, still open): 600 tasks take 31 ms, 5,000 take 1,373 ms. | Binary search over n, or sum per-task bytes once. | A | yes (timed) |
| F3 | Nit | src/mcp.ts:78 | `ref.slice(0,200)` on emoji text can end on a lone high surrogate (`isWellFormed()` is false). | Back off one code unit, or slice by code points. | A | yes (script) |
| F4 | Nit | README:76, SKILL.md:27 | Tasks cut into `tasks_omitted` cannot be listed through `get_status`, even for a named story (600 tasks: 259 shown, 341 omitted). The docs do not say how to see them. | Point to `replay_events {correlation_id, event_type: task_assigned}`. | A | yes (live) |
| N-B1 | Nit | test/status.test.ts:201-204, src/bus.ts:22,99 | Mutants `hb_ge` (`>` to `>=`) and `hb_skew_6s` pass 152/152. Tests use only +3 s and +30 s. The code is right: exactly +5000 ms is accepted and +5001 ms is ignored. | Assert that NOW+5000 counts and NOW+5001 does not. | B | yes (mutants and a live probe) |
| N-B2 | Nit | src/bus.ts:97 | Mutant `hb_no_story_null` passes 152/152. With it, a heartbeat with `correlation_id: null` on `amb.hb.P.null.T.A` is accepted. | Add the null-story test next to the existing null-task test (status.test.ts:208). | B | yes (probe) |
| N-B3 | Nit | src/mcp.ts:96 | Mutant `sl_cut_ge` (`> budget` to `>= budget`) passes 152/152. It matters only when the cut line is exactly at the budget. | Add an exact-budget assert for the `tasks_omitted` form. | B | yes (mutant) |
| Upgrade note | Info | event schema | Events stored by an older build with `correlation_id: "_"` now fail the schema. `read` reports them as invalid and `replay {correlation_id:"_"}` drops them. Matters only for a store created by an old build. | None needed for the prototype. | B | yes (`scripts/zz_reB_legacy.test.ts`) |

## Reviewer B: mutants and stress run

- **25 mutants, 21 killed, 4 survived** (all on copies under `re6-B/mut/`): the survivors are `sl_cut_ge`, `hb_no_story_null`, `hb_ge` and `hb_skew_6s` (see N-B1 to N-B3). Killed: `g1_no_share`, `g1_share_x2`, `sl_skip_level0` to `sl_skip_level3`, `sl_level_lt`, `sl_cut_extra`, `sl_cut_no_omitted`, `sl_cut_no_loop`, `hb_always_none`, `hb_no_type`, `hb_no_task_null`, `hb_no_subject`, `hb_no_skew_bound`, `hb_skew_0`, `hb_no_safeparse_strict`, `g3_no_refine`, `g3_identity_no_check`, `g3_identity_story_only`, `g3_identity_task_only`.
- **CPU stress:** full `bun test` on an unmodified copy with 15 `yes` processes running: 152 pass, 0 fail, 55.3 s, no flaky tests. P9 saw `stalled` 1023 ms after SIGKILL (bound 2600 ms). The `yes` processes were killed by pid.
- **Real heartbeats:** a live `amb mcp` planner, worker (AMB_HEARTBEAT_MS=200) and dashboard with AMB_STALL_AFTER_MS=800 showed T1 `started` with a fresh `lastHeartbeat` in both `get_status {correlation_id:S1}` and `get_status {}`.
- **heartbeatFrom edges:** a heartbeat dated one year in the past is accepted and reported `stalled`. An offset timestamp, a null story, story `_`, missing details, a missing `agent_id`, a wrong agent or project in the subject, and an extra subject token are all rejected.

## Checked and OK

- statusLine budget exactness (A, `re6-A/budget.ts`): 0 to 600 tasks, ASCII, Cyrillic, emoji and escape-heavy text, 64-character ids, large seqs, budgets from 50 to 49,000. No overshoot at any budget of 980 bytes or more. The smallest form (story header plus `tasks_omitted`) is 157 bytes at maximum sizes.
- The shrink order that README describes matches the statusLine levels.
- Legitimate flows still work after G3: story broadcasts, ad hoc events with no story, and a worker receiving the story broadcast through its `S1._.>` filter.

## Dismissed claims

None.

## Not checked

- B did not re-run the T2 replay-paging mutants or `pgrep_no_filter`; they rely on the author's mutant run.
- README and SKILL wording beyond D3, D4, D2 and the N-B4 clause.
- `heartbeatFrom` through a NATS server restart.
- `get_status` with `wait_ms` and `since_seq` across many stories.
- The cost of the O(n²) cut and the heartbeat key (N3), excluded from scope.

## Process note

At the first hand-back, reviewer B was still running, so reviewer A's agent returned an incomplete report. The orchestrating session waited for B and then merged both. All reviewer processes have ended, and the final `pgrep` check was empty.

## Known limitations and follow-ups across all rounds

- **Accepted limits:** no authentication of ids in stage 1 (a forged event on a matching subject still works); `cursor_status.gap` is approximate; a client that drops a result without sending a cancel is recovered with `replay_events own_scope=true`; the N3 variant (`wait_ms: 0` can cost about 1 s while un-ACKed deliveries are not yet due).
- **Deferred:** G2 (`get_task_context` hint after `per_type_limit` skips events), N2 (store-time heartbeat skew), N3 (heartbeat key and view mismatch), N4 and F2 (O(n²) cut).
- **New this round:** F1 (unused share not redistributed), F3 (emoji split), F4 (docs pointer for omitted tasks), the remaining D2 doc items, N-B1 to N-B3 test asserts.

## Recommended next actions

1. Fix D3: delete the "the note names them" clause from README.md:76 and SKILL.md:27, and ideally add a "showing N of M stories" header to `get_status`.
2. Fix D4: add one sentence on how the 50 stories are picked.
3. Optional, one line each: the N-B1 to N-B3 test asserts, and the D2 wording.
4. Approve as a stage-1 prototype after items 1 and 2. Keep F1 to F4 and the deferred items as stage-2 follow-ups.