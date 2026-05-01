use anyhow::{bail, Context, Result};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::Builder;

pub fn xcrun_path() -> Option<PathBuf> {
    which_in_path("xcrun")
}

pub fn xctrace_path() -> Result<String> {
    let output = Command::new("xcrun")
        .arg("--find")
        .arg("xctrace")
        .output()
        .context("failed to launch xcrun --find xctrace")?;

    if !output.status.success() {
        bail!(
            "xcrun --find xctrace failed: {}",
            compact_stderr(&output.stderr)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn xcode_version() -> Result<String> {
    let output = Command::new("xcodebuild")
        .arg("-version")
        .output()
        .context("failed to launch xcodebuild -version")?;

    if !output.status.success() {
        bail!(
            "xcodebuild -version failed: {}",
            compact_stderr(&output.stderr)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn xctrace_version() -> Result<String> {
    let output = Command::new("xcrun")
        .arg("xctrace")
        .arg("version")
        .output()
        .context("failed to launch xcrun xctrace version")?;

    if !output.status.success() {
        return Ok(format!(
            "unknown ({})",
            compact_stderr(&output.stderr).replace('\n', " ")
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn export_toc(trace: &Path) -> Result<Vec<u8>> {
    let tmp = Builder::new()
        .prefix("aitrace-xctrace-")
        .tempdir()
        .context("failed to create temporary directory for xctrace")?;

    let output = Command::new("xcrun")
        .env("TMPDIR", tmp.path())
        .arg("xctrace")
        .arg("export")
        .arg("--input")
        .arg(trace)
        .arg("--toc")
        .output()
        .with_context(|| {
            format!(
                "failed to launch xcrun xctrace export --toc for {}",
                trace.display()
            )
        })?;

    if !output.status.success() {
        bail!(
            "xctrace export --toc failed: {}",
            compact_stderr(&output.stderr)
        );
    }

    Ok(output.stdout)
}

pub fn export_xpath(trace: &Path, xpath: &str) -> Result<Vec<u8>> {
    let tmp = Builder::new()
        .prefix("aitrace-xctrace-")
        .tempdir()
        .context("failed to create temporary directory for xctrace")?;
    let out_path = tmp.path().join("export.xml");

    let output = Command::new("xcrun")
        .env("TMPDIR", tmp.path())
        .arg("xctrace")
        .arg("export")
        .arg("--input")
        .arg(trace)
        .arg("--xpath")
        .arg(xpath)
        .arg("--output")
        .arg(&out_path)
        .output()
        .with_context(|| {
            format!(
                "failed to launch xcrun xctrace export --xpath for {}",
                trace.display()
            )
        })?;

    if !output.status.success() {
        bail!(
            "xctrace export --xpath failed: {}",
            compact_stderr(&output.stderr)
        );
    }

    if out_path.exists() {
        std::fs::read(&out_path)
            .with_context(|| format!("xctrace did not create {}", out_path.display()))
    } else if !output.stdout.is_empty() {
        Ok(output.stdout)
    } else {
        bail!(
            "xctrace did not create {} and stdout was empty",
            out_path.display()
        )
    }
}

pub fn compact_stderr(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let mut lines = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains("First throw call stack")
            || line.contains("Last Exception Backtrace")
            || line.starts_with('(')
        {
            break;
        }

        lines.push(line.to_string());
        if lines.len() >= 6 {
            break;
        }
    }

    if lines.is_empty() {
        "no stderr".to_string()
    } else {
        lines.join(" | ")
    }
}

pub fn which_in_path(bin: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for dir in env::split_paths(&path) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}
