use crate::cli::OutputFormat;
use anyhow::{bail, Result};
use serde::Serialize;

pub fn print<T: Serialize>(format: OutputFormat, value: &T) -> Result<()> {
    match format {
        OutputFormat::AiYaml => {
            println!("{}", serde_yaml::to_string(value)?);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(value)?);
        }
        OutputFormat::Md => {
            bail!("markdown output is planned but not implemented in v0.1. Use --format ai-yaml or --format json");
        }
    }
    Ok(())
}

pub fn truncate_chars(input: &str, budget: usize) -> String {
    if input.chars().count() <= budget {
        return input.to_string();
    }

    let keep = budget.saturating_sub(32);
    let mut out = input.chars().take(keep).collect::<String>();
    out.push_str("… [truncated]");
    out
}

pub fn one_line(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}
