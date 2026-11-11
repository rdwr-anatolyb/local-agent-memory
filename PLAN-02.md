# PLAN-02: amb Windows fixes (sdlc-flow ticket 02, spec 4.1 and 9 item 6)

Scope: `src/server.ts`, `test/server.win.test.ts` (new), README and sdlc-flow spec 4.1 note. No Windows machine here: macOS can prove the logic with injected platform, never the OS behavior.
Existing callers: `main.ts:15-25` (`ensureServer`, `stopServer`, `serverStatus`), `test/harness.ts` (`findServerBin`, `stopServer`), proofs L6 and N13. Keep every export signature compatible by adding an optional last arg `deps`.

Bun docs checked (they say little about Windows):
- Bun.which: "By default Bun looks at the current PATH"; no mention of PATHEXT or .exe. https://bun.com/docs/runtime/utils
- Bun.spawn / unref: `proc.unref()` "detach the child"; no `detached` option documented, no Windows signal notes; Windows only appears for ConPTY and `windowsHide`. https://bun.com/docs/runtime/child-process
- SIGTERM on Windows terminates unconditionally (Node doc, assumed same in Bun, unchecked). https://nodejs.org/docs/latest/api/process.html#signal-events
- Detach bug: https://github.com/oven-sh/bun/issues/31603 (open, unconfirmed by us).

## Injection shape (shared by all fixes)
`type Deps = { platform?: NodeJS.Platform; run?: (cmd: string[]) => string; kill?: (pid: number, sig: string) => void; spawn?: typeof spawn; which?: (n: string) => string | null }`
Defaults are the real functions, so macOS/Linux call paths do not change. Same pattern as `psInfo(pid, {platform, run})` in sdlc-flow `src/cli/client.ts:66`. Path joins use `platform === "win32" ? path.win32.join : path.join` so tests build Windows paths on macOS.

## Fix 1: stopServer without ps/pgrep, matching nats-server.exe and -sd
Approach:
- Replace `argsOf(pid)` with `argsOf(pid, deps)`: unix runs `ps -p` as today; win32 runs `powershell -NoProfile -Command "(Get-CimInstance Win32_Process -Filter 'ProcessId=<pid>').CommandLine"`. A missing binary or non-zero exit returns "" (never throws).
- Replace the `pgrep` line with `pidsServing(home, deps)`: unix keeps `pgrep -f`; win32 runs `Get-CimInstance Win32_Process -Filter "Name='nats-server.exe'"` printing `<pid> <CommandLine>` per line, parsed in TS.
- `servesHome(args, home, platform)`: tokenise with a quote-aware split (Windows prints `"C:\Program Files\nats\nats-server.exe" -sd "C:\Users\x\.amb\jetstream"`). On win32 the binary test is `/(^|[\\/])nats-server(\.exe)?$/i` and the `-sd` compare lowercases and turns `/` into `\` on both sides. Unix test stays the strict regex.
- Kill: win32 SIGTERM is a hard kill. Accept it (JetStream recovers from kill; proof P5 covers SIGKILL on unix). Document in README.
Tests (`test/server.win.test.ts`, run with `bun test test/server.win.test.ts`), all with fake `run`/`kill`:
- servesHome true for quoted exe path with spaces and mixed-case `-SD` dir; false for another home, for `notepad.exe`, for `nats-server.exe.bak`.
- stopServer win32: pid file + fake `run` returning a matching CommandLine gives "stopped" and the fake kill saw the pid; foreign CommandLine gives "not-ours" and no kill; empty output gives "stale" and the pid file removed; `run` that throws gives "stale", no crash.
- Unix regression: existing L6 and N13 in `bun test test/proofs.test.ts`.
Unverified without Windows: PowerShell/CIM availability and exact CommandLine text (quoting, 8.3 short paths, drive-letter case), execution policy, 1-2 s PowerShell start cost, `-sd` as nats-server actually records it.

## Fix 2: detached spawn that survives its parent
Approach: repro first (step 4), then pick the lightest rung that works:
1. Today's `spawn(bin, args, {detached: true, stdio: ["ignore", log, log], windowsHide: true}).unref()`. Add only `windowsHide`; it is harmless on unix (ignored).
2. If the child dies with the parent: `spawn("cmd.exe", ["/c", "start", '""', "/B", bin, ...args])` with log redirected inside the command string, or `powershell Start-Process -WindowStyle Hidden -RedirectStandardOutput`. Pick the first that survives. Put it behind `spawnDetached(bin, args, log, deps)`, win32 only.
3. If none survives: document the limit (see Fallback).
Tests (macOS): `ensureServer` with fake `spawn` and `platform: "win32"` asserts the rung's argv/options (windowsHide set, `unref` called, `-sd` arg uses a backslash path); with `platform: "darwin"` asserts argv and options are byte-identical to today. Existing N13 (`ensureServer` then `stopServer`, real nats-server) covers unix.
Unverified: the whole point. Survival after the starter exits, handle inheritance of the log fd, console-window flash, job-object kill on terminal close or logoff.

Fallback if detach cannot work: keep the code at rung 1, make `ensureServer` on win32 print a one-line warning ("server lives only while this process runs"), document in README that on Windows the bus must be started by a long-lived process (sdlc-flow's own `startApp` is one), and edit sdlc-flow spec 4.1 so Windows stays on the in-memory backend. Do not claim `server start/stop` work on Windows in that case.

## Fix 3: install hint without brew on Windows
Approach: `installHint(platform)`: win32 gives `winget install NATS.Server` or `choco install nats-server` or the release zip, plus "set AMB_NATS_SERVER_BIN to the .exe"; others keep `brew install nats-server`. Check the winget id at https://github.com/nats-io/nats-server/releases and winget search before writing it; if unconfirmed, name only the release zip and choco. `findServerBin` uses it.
Tests: `findServerBin({}, {platform: "win32", which: () => null})` throws a message that has no "brew" and names `AMB_NATS_SERVER_BIN`; darwin message still contains `brew install nats-server`.
Unverified: that the named package manager id installs a runnable binary.

## Fix 4: Bun.which resolving nats-server.exe, or AMB_NATS_SERVER_BIN
Approach: `findServerBin` tries `which("nats-server")`, then on win32 `which("nats-server.exe")`; `AMB_NATS_SERVER_BIN` still wins first. The `.exe` retry is cheap and removes the PATHEXT question, so no verdict on Bun is needed. Spawn gets the resolved absolute path.
Tests: fake `which` that returns null for the bare name and a path for `nats-server.exe` resolves on win32 and not on darwin (darwin throws); env override beats both; empty env var is ignored.
Unverified: whether bare `Bun.which("nats-server")` already works on Windows (docs silent). Run the one-liner `bun -e "console.log(Bun.which('nats-server'), Bun.which('nats-server.exe'))"` on Windows and record the result in the README.

## Ordered steps (each ends with its test; all on macOS unless marked)
1. Add `Deps` and `installHint`, wire `findServerBin` (fixes 3 and 4) and tests. Run `bun test test/server.win.test.ts`, then `bun test`.
2. Add win32 `argsOf`, `pidsServing`, quote-aware `servesHome`, injected `kill` (fix 1) and tests. Run `bun test test/server.win.test.ts test/proofs.test.ts` (L6, N13).
3. Add `spawnDetached` with `windowsHide` and the injected `spawn`; add argv-equality test for darwin. Run `bun test`; also `bunx tsc --noEmit`.
4. WINDOWS ONLY: write `scripts/win-smoke.ps1` (runs `bun src/main.ts server start`, waits for exit, checks `server status`, then `stop`, then checks the port is closed and `nats.pid` is gone). Run it on a Windows box; paste output into the PR. Also run the bun#31603 repro there before choosing the rung in step 3; if rung 1 fails, redo step 3 with rung 2, then rung 3.
5. README "Windows" section: what was verified (and on which Windows and Bun version), hard-kill stop, the detach limit if any. If the limit stands, edit sdlc-flow `docs/FLOW_ENGINE_SPEC.md` 4.1 and ticket 02's third box.

## Acceptance mapping
- start/status/stop on Windows, detached survives starter: only step 4 can tick these boxes. Without it the ticket stays open, and the PR says "unit-proven on macOS, not run on Windows".
- macOS/Linux unchanged: `bun test` green plus the darwin argv-equality test.
