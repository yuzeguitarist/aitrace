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

For private repos or a non-default URL:

```bash
AITRACE_GIT_URL="https://github.com/yuzeguitarist/aitrace.git" bash install.sh
```

The script builds with `cargo install --path` and installs the `aitrace` binary
into Cargo's bin directory, usually `~/.cargo/bin`.

## Quick start

Preferred AI one-shot command:

```bash
aitrace diagnose /path/to/Deck.trace --target Deck --repo /path/to/Deck --budget 1800
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
aitrace inspect /path/to/Deck.trace --format ai-yaml
```

Build the cache:

```bash
aitrace index /path/to/Deck.trace --preset cpu
```

Get the small AI summary:

```bash
aitrace summary /path/to/Deck.trace --preset cpu --target Deck --budget 1200
```

For Energy Organizer / Activity Monitor style traces:

```bash
aitrace summary /path/to/Deck.trace --preset energy --target Deck --budget 1600
```

For mixed system/runtime traces that contain GCD, syscall, thread-state,
runloop, region-of-interest, or narrative tables:

```bash
aitrace summary /path/to/Deck.trace --preset diagnostics --target Deck --budget 1600
```

Drill into a finding:

```bash
aitrace drill /path/to/Deck.trace cpu.hotspot.1 --depth 8 --budget 1000
```

Or drill by exact evidence ID:

```bash
aitrace drill /path/to/Deck.trace ev:cpu:run1:table53:row4428 --depth 8
```

Print bounded raw evidence only when necessary:

```bash
aitrace raw /path/to/Deck.trace ev:cpu:run1:table53:row4428 --context 4 --budget 4000
```

## AI agent usage rule

Put this in `AGENTS.md` / project instructions for Codex, Claude Code, Cursor,
or other terminal-using agents:

```md
When analyzing Apple Instruments `.trace` files, never run `xctrace` directly
and never `cat` exported XML.

Use:

- `aitrace doctor`
- `aitrace diagnose <trace> --target <AppName> --repo <repo> --budget 1800`
- `aitrace inspect <trace> --format ai-yaml`
- `aitrace summary <trace> --preset cpu --target <AppName> --budget 1200`
- `aitrace summary <trace> --preset diagnostics --target <AppName> --budget 1600`
- `aitrace summary <trace> --preset energy --target <AppName> --budget 1600`
- `aitrace summary <trace> --preset hangs --target <AppName> --budget 1200`
- `aitrace find <trace> --symbol <SymbolName> --budget 1200`
- `aitrace drill <trace> <finding-or-evidence-id> --depth 8 --budget 1000`
- `aitrace raw <trace> <evidence-id> --context 4 --budget 4000`

Only use `aitrace export` for parser debugging.
Do not paste raw `xctrace` XML into model context.
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
export AITRACE_CACHE_DIR=/path/to/cache
```

The cache key includes trace metadata, parser version, and the detected
`xctrace` version. Rebuild explicitly:

```bash
aitrace index /path/to/Deck.trace --force
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

## GitHub hosting

Recommended first push:

```bash
git init
git add .
git commit -m "Initial aitrace CLI"
gh repo create aitrace --public --source=. --remote=origin --push
```

The current public repo URL is `https://github.com/yuzeguitarist/aitrace`.

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
