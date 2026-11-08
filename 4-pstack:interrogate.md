# Agent Memory Bus round-6 final check (job-15 re-run)

**Verdict.** Ready to approve as a stage-1 prototype, on one condition. D3, D4, F3, N-B1 to N-B3 and D2 are closed and work in a live `amb mcp`. F4 is only partly closed (N1 below). Its fix is a one-line change to the note text: land it before sign-off, or track it. N2 to N4 can be follow-ups.

**Reviewer model.** One `opus` reviewer, run through `/pstack:interrogate`. It finished before this report was written and returned 5 findings: 0 critical, 3 warning, 2 nit. I reproduced its warnings 1 to 3 live, and they appear below as N1 to N3. Orchestrator checks: `tsc --noEmit` clean and `bun test` 158 pass, 0 fail (46 s) before this review.

## Round-6 items

| Item | Status | Evidence |
|---|---|---|
| D3 (`statusPage` returns `{stories, active}`, header) | closed | Live run through `amb mcp` with a temp nats-server: <br>• 50 stories: no header, 50 lines. <br>• 51 stories: the first line is exactly `showing 50 of 51 stories with the most recent task activity, most recent first; pass correlation_id, or a narrower since, for the others`. <br>• 58 stories: header says `showing 50 of 58…` and 50 stories are shown. The order is the expected newest-activity list (`s20, s10, s03, s57…`, last `s09`). The whole text is 11,737 bytes, under 50,000. <br>• Named-story call: no header. <br>• Future `since`: no stories, no header. <br>• `getStatus` wraps `.stories` (bus.ts:439-441). The stale "note names them" text is gone (grep finds nothing). |
| D4 (docs: the 50 stories with the most recent task activity, most recent first) | closed | README.md:76, SKILL.md:27 and the `get_status` description (mcp.ts:189-194) all match the code in bus.ts:461-469. |
| N-B1 (heartbeat skew +5000 counts, +5001 does not) | closed | status.test.ts:207-210 |
| N-B2 (null-story heartbeat ignored) | closed | status.test.ts:211-212 |
| N-B3 (cut form inclusive at exact budget) | closed | status.test.ts:177-182 |
| D2 (wording) | closed | I assumed D2 means the `get_decisions` description. It says "oldest first, the newest `limit` (default 50); if cut, the newest are kept". That matches `replay` with `slice(-limit)` and `wrapped(keep: "newest")`. |
| F3 (artifact refs cut by code point, max 200) | closed | Live run with 6 refs: astral emoji at odd and even offsets, 199 ASCII characters followed by emoji, `é`, and ZWJ family sequences. Every ref came back at exactly 200 code points, as a prefix of the original, with no lone surrogate. This held for both the named and the unnamed `get_status` paths. |
| F4 (per-story note pointing at `replay_events`) | partial | The note is present and correct when a story is cut. With 600 tasks and long ids, 426 tasks were shown, 174 omitted, and the note appeared. With 600 short ids nothing is cut and no note appears, which is correct. But the call the note names does not "list them all". See N1. |

## New findings

| Id | Severity | Location | Repro | Fix | Verified |
|---|---|---|---|---|---|
| N1 | medium | mcp.ts:112 (note text); mcp.ts:172 (`limit ?? 200`, `keep: "newest"`); bus.ts:401; README.md:76; SKILL.md:27 | Publish 1000 `task_assigned` events (64-char task ids) into story `big`. <br>• `get_status {correlation_id: "big"}` lists 426 tasks with `tasks_omitted: 574`. <br>• The note's call `replay_events {correlation_id:"big", event_type:"task_assigned"}` returns the newest 102 of 200. Following its hint (`from_seq=801 limit=98`) gives 98 more and then stops. <br>• Status plus every hint reaches 626 of 1000 tasks. The 374 tasks between them are unreachable, and nothing says more exist. <br>• Planner retries also add `task_assigned` events, so the count is per event, not per task. | Make the note say `replay_events {correlation_id, event_type: "task_assigned", from_seq: 1, limit: 1000}` and tell the agent to follow the `continue with from_seq` hints. Update README and SKILL.md the same way. Add a test that runs the named call and counts distinct `task_id`s. | Yes, live. The fixed call reached 1000 of 1000 tasks. Opus raised the same issue on its own. |
| N2 | low | mcp.ts:105-113 (per-story notes are not counted in `LINES_BUDGET`) | 51 stories with 12 tasks each and 64-char ids. All 50 shown stories are cut, so 50 notes are added. The `get_status` text is **57,195 bytes**, over the ~50 KB contract in untrusted.ts:16. | Subtract the note bytes before splitting the share, or merge the notes into one note that lists the cut story ids. | Yes, live. Opus predicted about 58 to 61 KB. |
| N3 | low | bus.ts:465 vs status.ts:85 | One assigned story plus one story whose only event is a worker `error`. The header says `showing 1 of 2 stories…`, but passing that other correlation_id returns nothing. With more than 50 active stories, such an empty story can also take one of the 50 places. | Count a story as active only when it has a `task_assigned` or `task_started` event. Or derive `active` from the stories that actually produce a status. | Yes, live. Opus raised it too. |
| N4 | low (test gap) | test/proofs.test.ts:1154 (D3 uses only 58 stories) | Mutants `header_plus1`, `order_first_activity` and `active_no_task_filter` survive. See the mutants section. | Add three tests: exactly 51 active stories shows the header; an old story that becomes active again moves to the front; a story with only a no-task `error` is not counted. | Yes. The suite stays green with each mutant applied. |
| N5 | nit | mcp.ts:100-102 | The cut keeps the tasks in first-event order, so the newest tasks are the ones dropped. The `get_status` tool description does not mention `tasks_omitted`. | Keep the tasks with the newest `lastEventSeq`, or the non-terminal ones, first. Add one clause about `tasks_omitted` to the description. | Read from the code only (opus). |

## Mutants and survivors

I ran 11 new mutants, each on a copy under the scratchpad, against the full suite. The unmutated suite ran once: 158/158 pass, 505 expects, 47 s.

| Mutant | Result |
|---|---|
| `active_picked` (`active: picked.length`) | killed (D3) |
| `header_ge` (`>=`, header shown at 50 of 50) | killed (D3) |
| **`header_plus1`** (`active > stories.length + 1`, so no header at 51) | **survived** |
| `named_active` (named path `active + 1`) | killed (D3) |
| `f4_wrong_type` (note says `task_started`) | killed (F4) |
| `f4_wrong_id` (note shows a placeholder id) | killed (F4) |
| `order_oldest` (sort ascending) | killed (N8, D3) |
| **`order_first_activity`** (story ranked by its first activity in the window, not its latest) | **survived** |
| `cp_201` (cut at 201 code points) | killed (F3, R3) |
| `cp_199` (cut at 199 code points) | killed (F3) |
| **`active_no_task_filter`** (drops the `e.task_id` check) | **survived** |

The live runs above show the real code behaves correctly at the 50/51 boundary and orders stories by latest activity. Only the tests miss these cases (N4).

## Not checked

- Concurrency and long-poll (`wait_ms`) behaviour of `statusPage`.
- Performance of the window scan on a large stream.
- The opus reviewer's claim about ad hoc tasks. It says tasks started by an ad hoc `task_started` with no assignment are not covered by the F4 call. That follows from the code but was not run.
- I wrote the D2 row on the assumption that D2 refers to the `get_decisions` description.
- I made no edits to the source and no commits. The worktree `src/` and `test/` match my copies byte for byte.
- `pgrep -fl 'bin/nats-server|bun test|mutate|main.ts mcp|^yes'` is empty. No process runs from the scratch dir, and the mutant copies and temp homes were removed.

## Remaining known limitations and follow-ups

- **Accepted stage-1 limits:**
  - No auth or id verification.
  - `cursor_status.gap` is approximate.
  - N3 variant latency.
  - Cancel-without-cancel recovery goes through `replay_events from_seq`.
- **Deferred from earlier rounds:** G2, N2, N3, N4, F1, F2. These are the earlier rounds' ids, not the new N2 to N4 above.
- **New from this round:**
  - N1: fix the F4 note, README and SKILL.md wording, and add a test that runs the named call.
  - N2: count the notes in the 50 KB budget.
  - N3: count as active only stories that have an attempt.
  - N4: add the three missing tests.
  - N5: keep the newest tasks when cutting, and mention `tasks_omitted` in the tool description.
- **Related:** `replay_events` without `from_seq` drops anything past its `limit` silently, with no "more exist" signal. N1 shows this, and it applies to every `replay_events` caller, not only F4.