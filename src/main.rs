mod analyzer;
mod cache;
mod cli;
mod doctor;
mod output;
mod toc;
mod xctrace;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Doctor { format } => doctor::run(format),
        Commands::Inspect {
            trace,
            format,
            limit,
        } => analyzer::inspect(trace, format, limit),
        Commands::Index {
            trace,
            preset,
            run,
            force,
            format,
            limit_rows_per_table,
        } => analyzer::index(trace, preset, run, force, format, limit_rows_per_table),
        Commands::Summary {
            trace,
            preset,
            run,
            target,
            thread,
            budget,
            limit,
            show_system,
            no_auto_index,
            format,
        } => analyzer::summary(
            trace,
            preset,
            run,
            target,
            thread,
            budget,
            limit,
            !show_system,
            no_auto_index,
            format,
        ),
        Commands::Diagnose {
            trace,
            target,
            run,
            repo,
            budget,
            limit,
            show_system,
            no_auto_index,
            format,
        } => analyzer::diagnose(
            trace,
            target,
            run,
            repo,
            budget,
            limit,
            !show_system,
            no_auto_index,
            format,
        ),
        Commands::Find {
            trace,
            preset,
            symbol,
            module,
            thread,
            regex,
            budget,
            limit,
            no_auto_index,
            format,
        } => analyzer::find(
            trace,
            preset,
            symbol,
            module,
            thread,
            regex,
            budget,
            limit,
            no_auto_index,
            format,
        ),
        Commands::Drill {
            trace,
            id,
            preset,
            depth,
            budget,
            no_auto_index,
            format,
        } => analyzer::drill(trace, id, preset, depth, budget, no_auto_index, format),
        Commands::Raw {
            trace,
            evidence,
            context,
            budget,
            no_auto_index,
            format,
        } => analyzer::raw(trace, evidence, context, budget, no_auto_index, format),
        Commands::Export { trace, xpath } => analyzer::export(trace, xpath),
    }
}
