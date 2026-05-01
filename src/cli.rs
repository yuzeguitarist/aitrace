use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "aitrace")]
#[command(version)]
#[command(about = "AI-friendly analyzer for Apple Instruments .trace bundles")]
#[command(
    long_about = "aitrace turns noisy xctrace XML exports into small, deterministic AI-readable summaries with evidence IDs and drill-down commands."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Check Xcode/xctrace/cache/tmp prerequisites.
    Doctor {
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// Inspect a .trace file and list available runs/tables/schemas.
    Inspect {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,

        /// Maximum schema rows to print.
        #[arg(long, default_value_t = 120)]
        limit: usize,
    },

    /// Build or refresh the local searchable index for useful tables.
    Index {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Preset to index. Omit to index all known AI-useful presets.
        #[arg(long, value_enum)]
        preset: Option<Preset>,

        /// Run number to index, or "latest". Omit to index all runs.
        #[arg(long)]
        run: Option<String>,

        /// Re-export tables even if a matching cache DB exists.
        #[arg(long)]
        force: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,

        /// Safety cap for row extraction per table. Raw table XML is still stored.
        #[arg(long, default_value_t = 200_000)]
        limit_rows_per_table: usize,
    },

    /// Print a small AI-budget summary for a preset.
    Summary {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Analysis preset.
        #[arg(long, value_enum, default_value_t = Preset::Cpu)]
        preset: Preset,

        /// Run number to summarize, or "latest". Omit to summarize all indexed runs.
        #[arg(long)]
        run: Option<String>,

        /// Target process/app name filter.
        #[arg(long)]
        target: Option<String>,

        /// Thread name/id text filter.
        #[arg(long)]
        thread: Option<String>,

        /// Approximate character budget for output.
        #[arg(long, default_value_t = 1200)]
        budget: usize,

        /// Maximum findings to print.
        #[arg(long, default_value_t = 12)]
        limit: usize,

        /// Show obvious system framework rows instead of folding them.
        #[arg(long)]
        show_system: bool,

        /// Do not build the index automatically when missing.
        #[arg(long)]
        no_auto_index: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// One-shot, ultra-compact AI RCA report. Fast path: CPU + main-thread evidence.
    Diagnose {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Target process/app name filter.
        #[arg(long)]
        target: Option<String>,

        /// Run number to diagnose, "latest", or "all". Defaults to latest to avoid huge all-run traces.
        #[arg(long, default_value = "latest")]
        run: String,

        /// Repository root for best-effort Swift source file/line hints.
        #[arg(long)]
        repo: Option<PathBuf>,

        /// Approximate character budget for output.
        #[arg(long, default_value_t = 1800)]
        budget: usize,

        /// Maximum compact hotspot lines.
        #[arg(long, default_value_t = 8)]
        limit: usize,

        /// Show obvious system framework rows instead of folding them.
        #[arg(long)]
        show_system: bool,

        /// Do not build the index automatically when missing.
        #[arg(long)]
        no_auto_index: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// Search indexed rows by symbol/module/thread/regex.
    Find {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Optional preset scope.
        #[arg(long, value_enum)]
        preset: Option<Preset>,

        /// Symbol/function text to search.
        #[arg(long)]
        symbol: Option<String>,

        /// Module/library/app text to search.
        #[arg(long)]
        module: Option<String>,

        /// Thread text to search.
        #[arg(long)]
        thread: Option<String>,

        /// Regex to match against flattened row text.
        #[arg(long)]
        regex: Option<String>,

        /// Approximate character budget for output.
        #[arg(long, default_value_t = 1200)]
        budget: usize,

        /// Maximum matches to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Do not build the index automatically when missing.
        #[arg(long)]
        no_auto_index: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// Expand one finding/evidence ID with nearby indexed evidence.
    Drill {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Finding ID such as cpu.hotspot.1, or evidence ID such as ev:cpu:run1:table3:row42.
        id: String,

        /// Preset used when resolving a finding ID.
        #[arg(long, value_enum, default_value_t = Preset::Cpu)]
        preset: Preset,

        /// Number of neighboring rows to show around row evidence.
        #[arg(long, default_value_t = 8)]
        depth: usize,

        /// Approximate character budget for output.
        #[arg(long, default_value_t = 1000)]
        budget: usize,

        /// Do not build the index automatically when missing.
        #[arg(long)]
        no_auto_index: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// Print bounded raw evidence for an evidence ID.
    Raw {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// Evidence ID from summary/find/drill.
        evidence: String,

        /// Number of neighboring rows to include for row evidence.
        #[arg(long, default_value_t = 0)]
        context: usize,

        /// Approximate character budget for output.
        #[arg(long, default_value_t = 4000)]
        budget: usize,

        /// Do not build the index automatically when missing.
        #[arg(long)]
        no_auto_index: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::AiYaml)]
        format: OutputFormat,
    },

    /// Export a selected XPath through xctrace. Debug escape hatch; not recommended for AI loops.
    Export {
        /// Path to an Instruments .trace bundle.
        trace: PathBuf,

        /// XPath accepted by xctrace export --xpath.
        #[arg(long)]
        xpath: String,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputFormat {
    AiYaml,
    Json,
    Md,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Preset {
    Overview,
    Cpu,
    Diagnostics,
    Energy,
    Hangs,
    Memory,
    Oslog,
}

impl Preset {
    pub fn as_str(self) -> &'static str {
        match self {
            Preset::Overview => "overview",
            Preset::Cpu => "cpu",
            Preset::Diagnostics => "diagnostics",
            Preset::Energy => "energy",
            Preset::Hangs => "hangs",
            Preset::Memory => "memory",
            Preset::Oslog => "oslog",
        }
    }

    pub fn schema_needles(self) -> &'static [&'static str] {
        match self {
            Preset::Overview => &[],
            Preset::Cpu => &[
                "cpu-profile",
                "time-profile",
                "time profiler",
                "sample",
                "call tree",
            ],
            Preset::Diagnostics => &[
                "gcd-perf-event",
                "syscall",
                "context-switch",
                "thread-state",
                "thread-snapshot",
                "thread-narrative",
                "system-load",
                "cpu-state",
                "cpu-narrative",
                "runloop-events",
                "life-cycle-period",
                "region-of-interest",
                "roi-metadata",
                "global-poi-layout",
                "global-roi-layout",
            ],
            Preset::Energy => &[
                "activity-monitor-process-live",
                "activity-monitor-process-ledger",
                "activity-monitor-system",
                "sysmon-process",
                "sysmon-system",
                "device-thermal-state",
            ],
            Preset::Hangs => &[
                "hang-risks",
                "potential-hangs",
                "hitches",
                "hitches-updates",
                "swiftui-update",
                "swiftui-causes",
            ],
            Preset::Memory => &[
                "allocations",
                "leaks",
                "vm-tracker",
                "virtual-memory",
                "memory",
                "regions",
            ],
            Preset::Oslog => &["os-log", "signpost", "points-of-interest", "logging"],
        }
    }

    pub fn all_index_presets() -> &'static [Preset] {
        &[
            Preset::Cpu,
            Preset::Diagnostics,
            Preset::Energy,
            Preset::Hangs,
            Preset::Memory,
            Preset::Oslog,
        ]
    }
}
