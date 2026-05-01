# aitrace

`aitrace` is an AI-friendly command-line analyzer for Apple Instruments `.trace`
bundles. It keeps raw `xctrace` XML out of the model context by turning a trace
into a local searchable cache, then printing small deterministic summaries with
evidence IDs.

The design goal is not “XML to Markdown”. The design goal is:

```text
.trace bundle
  -> xctrace export
  -> local SQLite + compressed raw evidence
  -> tiny AI-budget summary
  -> drill/raw only when the AI needs exact proof
```

## Status

This repository contains the first CLI version:

- `aitrace doctor`
- `aitrace diagnose`
- `aitrace inspect`
- `aitrace index`
- `aitrace summary --preset cpu|diagnostics|energy|hangs|memory|oslog`
- `aitrace find`
- `aitrace drill`
- `aitrace raw`
- `aitrace export` as a debug escape hatch

MCP is intentionally left for a later version.

## Requirements

- macOS
- Xcode command line tools with `xcrun xctrace`
- Rust toolchain for source install (`cargo`)

Check your machine:

```bash
aitrace doctor
```

## Install

### Local source install

From this repo:

```bash
bash install.sh
```

Then:

```bash
aitrace doctor
```

### One-command install script

Install from the public GitHub repo:

```bash
curl -fsSL https://raw.githubusercontent.com/yuzeguitarist/aitrace/main/install.sh | bash
```

The script builds with `cargo install --path` and installs the `aitrace` binary
into Cargo's bin directory, usually `~/.cargo/bin`.

## Quick start

Start with a local `.trace` bundle in the current folder, for example
`Deck.trace`.

Preferred AI one-shot command:

```bash
aitrace diagnose Deck.trace --target Deck --repo . --budget 1800
```

`diagnose` defaults to `--run latest` so a giant multi-run trace does not
silently index every run. It prints compact RCA lines only:

- `rca`: likely root cause in one or two lines
- `top`: top CPU hotspots
- `main`: main-thread CPU hotspots
- `swift`: async/closure/MainActor hints when present
- `src`: best-effort Swift source file/line hints when `--repo` is provided
- `ev`: evidence IDs for drill-down

Inspect a trace without exporting giant XML into the AI context:

```bash
aitrace inspect Deck.trace --format ai-yaml
```

`summary`, `find`, `drill`, and `raw` auto-build the cache when needed. Use
targeted summaries for follow-up:

```bash
aitrace summary Deck.trace --preset cpu --target Deck --run latest --budget 1200
```

For Energy Organizer / Activity Monitor style traces:

```bash
aitrace summary Deck.trace --preset energy --target Deck --run latest --budget 1600
```

For mixed system/runtime traces that contain GCD, syscall, thread-state,
runloop, region-of-interest, or narrative tables:

```bash
aitrace summary Deck.trace --preset diagnostics --target Deck --run latest --budget 1600
```

Drill into a finding:

```bash
aitrace drill Deck.trace cpu.hotspot.1 --preset cpu --depth 8 --budget 1000
```

Print bounded raw evidence only when necessary:

```bash
aitrace raw Deck.trace ev:cpu:run1:table53:row4428 --context 4 --budget 4000
```

## AI agent usage rule

Put this in `AGENTS.md` / project instructions for Codex, Claude Code, Cursor,
or other terminal-using agents:

```md
When analyzing Apple Instruments `.trace` bundles:

1. Do not run `xcrun xctrace` directly, and do not paste exported XML into the
   model context.
2. If the environment is unknown, run `aitrace doctor` first.
3. Start with `aitrace diagnose TRACE.trace --target APP --repo . --budget 1800`.
   `diagnose` defaults to `--run latest`; use `--run all` only when explicitly
   needed.
4. If the trace contents are unknown, run
   `aitrace inspect TRACE.trace --format ai-yaml`.
5. Use targeted follow-up summaries, usually with `--run latest`:
   - `aitrace summary TRACE.trace --preset cpu --target APP --run latest --budget 1200`
   - `aitrace summary TRACE.trace --preset diagnostics --target APP --run latest --budget 1600`
   - `aitrace summary TRACE.trace --preset energy --target APP --run latest --budget 1600`
   - `aitrace summary TRACE.trace --preset hangs --target APP --run latest --budget 1200`
6. Use `aitrace find`, `aitrace drill`, and `aitrace raw` only with symbols,
   finding IDs, or evidence IDs produced by `aitrace`.
7. Use `aitrace export` only for parser debugging, never as the normal AI loop.
```

## Cache

By default, caches live at:

```text
~/Library/Caches/aitrace
```

If that directory is not writable in a sandboxed AI runner, `aitrace` falls back
to `${TMPDIR}/aitrace-cache`.

Override with:

```bash
export AITRACE_CACHE_DIR="$HOME/aitrace-cache"
```

The cache key includes trace metadata, parser version, and the detected
`xctrace` version. Rebuild explicitly:

```bash
aitrace index Deck.trace --force
```

## Output contract

In AI mode (`--format ai-yaml`, the default):

- stdout contains only structured result data
- `diagnose` intentionally uses short keys and line strings to minimize tokens
- `xctrace` stderr is compacted into normalized errors
- full XML is never printed by `summary`/`find`/`drill`
- every summary finding contains an `evidence` ID
- every finding includes a suggested next command
- `raw` output is budget-limited

## Development

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```

The current parser is intentionally conservative: it indexes useful schemas
selected from `xctrace export --toc`, stores raw table/row evidence compressed,
and scores XML rows generically. Future versions can add schema-specific parsers
for Time Profiler call trees, hangs, allocations, signposts, and OSLog.

## License

MIT © 2026 Yuze Pan
