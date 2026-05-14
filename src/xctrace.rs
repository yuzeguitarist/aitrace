use anyhow::{bail, Context, Result};
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use tempfile::{Builder, TempDir};

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
    let output = run_export_toc(trace)?;
    if output.status.success() || looks_like_xml(&output.stdout, b"<trace-toc") {
        return Ok(output.stdout);
    }

    let staged = stage_trace(trace)?;
    let staged_trace = staged.path().join("input.trace");
    let retry = run_export_toc(&staged_trace)?;
    if retry.status.success() || looks_like_xml(&retry.stdout, b"<trace-toc") {
        return Ok(retry.stdout);
    }

    bail!(
        "xctrace export --toc failed: {}",
        compact_failure(&retry.status, &retry.stderr)
    );
}

fn run_export_toc(trace: &Path) -> Result<std::process::Output> {
    let tmp = Builder::new()
        .prefix("aitrace-xctrace-")
        .tempdir()
        .context("failed to create temporary directory for xctrace")?;

    let output = Command::new("xcrun")
        .env("TMPDIR", tmp.path())
        .arg("xctrace")
        .arg("export")
        .arg("--quiet")
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

    Ok(output)
}

pub fn export_xpath(trace: &Path, xpath: &str) -> Result<Vec<u8>> {
    let (status, xml, stderr) = run_export_xpath(trace, xpath)?;
    if status.success() || looks_like_xml(&xml, b"<trace-query-result") {
        return Ok(xml);
    }

    let staged = stage_trace(trace)?;
    let staged_trace = staged.path().join("input.trace");
    let (retry_status, retry_xml, retry_stderr) = run_export_xpath(&staged_trace, xpath)?;
    if retry_status.success() || looks_like_xml(&retry_xml, b"<trace-query-result") {
        return Ok(retry_xml);
    }

    let stderr = if retry_stderr.is_empty() {
        stderr
    } else {
        retry_stderr
    };
    bail!(
        "xctrace export --xpath failed: {}",
        compact_failure(&retry_status, &stderr)
    );
}

fn run_export_xpath(trace: &Path, xpath: &str) -> Result<(ExitStatus, Vec<u8>, Vec<u8>)> {
    let tmp = Builder::new()
        .prefix("aitrace-xctrace-")
        .tempdir()
        .context("failed to create temporary directory for xctrace")?;
    let out_path = tmp.path().join("export.xml");
    let stdout = File::create(&out_path)
        .with_context(|| format!("failed to create {}", out_path.display()))?;

    let output = Command::new("xcrun")
        .env("TMPDIR", tmp.path())
        .arg("xctrace")
        .arg("export")
        .arg("--quiet")
        .arg("--input")
        .arg(trace)
        .arg("--xpath")
        .arg(xpath)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "failed to launch xcrun xctrace export --xpath for {}",
                trace.display()
            )
        })?
        .wait_with_output()
        .context("failed to wait for xcrun xctrace export --xpath")?;

    let xml = if out_path.exists() {
        fs::read(&out_path).with_context(|| format!("failed to read {}", out_path.display()))?
    } else {
        Vec::new()
    };
    Ok((output.status, xml, output.stderr))
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

fn compact_failure(status: &ExitStatus, stderr: &[u8]) -> String {
    let compact = compact_stderr(stderr);
    if compact == "no stderr" {
        format!("{status}; no stderr")
    } else {
        format!("{status}; {compact}")
    }
}

fn looks_like_xml(bytes: &[u8], marker: &[u8]) -> bool {
    !bytes.is_empty() && bytes.windows(marker.len()).any(|window| window == marker)
}

fn stage_trace(trace: &Path) -> Result<TempDir> {
    let stage = Builder::new()
        .prefix("aitrace-xctrace-input-")
        .tempdir()
        .context("failed to create temporary trace staging directory")?;
    let dst = stage.path().join("input.trace");
    copy_path(trace, &dst).with_context(|| format!("failed to stage trace {}", trace.display()))?;
    Ok(stage)
}

fn copy_path(src: &Path, dst: &Path) -> Result<()> {
    let meta =
        fs::symlink_metadata(src).with_context(|| format!("failed to stat {}", src.display()))?;
    if meta.is_dir() {
        fs::create_dir_all(dst).with_context(|| format!("failed to create {}", dst.display()))?;
        for entry in fs::read_dir(src)
            .with_context(|| format!("failed to read directory {}", src.display()))?
        {
            let entry = entry?;
            copy_path(&entry.path(), &dst.join(entry.file_name()))?;
        }
    } else if meta.file_type().is_symlink() {
        let target = fs::read_link(src)
            .with_context(|| format!("failed to read symlink {}", src.display()))?;
        let target = if target.is_absolute() {
            target
        } else {
            src.parent().unwrap_or_else(|| Path::new(".")).join(target)
        };
        copy_path(&target, dst)?;
    } else if meta.is_file() {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::copy(src, dst)
            .with_context(|| format!("failed to copy {} to {}", src.display(), dst.display()))?;
    }
    Ok(())
}
