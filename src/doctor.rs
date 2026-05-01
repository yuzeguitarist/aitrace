use crate::cli::OutputFormat;
use crate::output;
use crate::{cache, xctrace};
use anyhow::Result;
use serde::Serialize;
use std::fs;

#[derive(Debug, Serialize)]
struct DoctorOutput {
    kind: &'static str,
    status: &'static str,
    checks: Vec<Check>,
    supported_presets: Vec<&'static str>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Check {
    name: &'static str,
    status: &'static str,
    detail: String,
}

pub fn run(format: OutputFormat) -> Result<()> {
    let mut checks = Vec::new();

    match xctrace::xcrun_path() {
        Some(path) => checks.push(ok("xcrun", path.display().to_string())),
        None => checks.push(fail("xcrun", "not found in PATH")),
    }

    match xctrace::xctrace_path() {
        Ok(path) => checks.push(ok("xctrace", path)),
        Err(err) => checks.push(fail("xctrace", err.to_string())),
    }

    match xctrace::xcode_version() {
        Ok(version) => checks.push(ok("xcode", version)),
        Err(err) => checks.push(fail("xcode", err.to_string())),
    }

    match xctrace::xctrace_version() {
        Ok(version) => checks.push(ok("xctrace_version", version)),
        Err(err) => checks.push(warn("xctrace_version", err.to_string())),
    }

    let tmpdir = std::env::temp_dir();
    let tmp_status = fs::create_dir_all(&tmpdir)
        .and_then(|_| {
            tempfile::Builder::new()
                .prefix("aitrace-doctor-")
                .tempdir_in(&tmpdir)
        })
        .map(|dir| dir.path().display().to_string());
    match tmp_status {
        Ok(path) => checks.push(ok("tmpdir", path)),
        Err(err) => checks.push(fail("tmpdir", err.to_string())),
    }

    match cache::ensure_cache_dir() {
        Ok(path) => checks.push(ok("cache", path.display().to_string())),
        Err(err) => checks.push(fail("cache", err.to_string())),
    }

    let failed = checks.iter().any(|check| check.status == "fail");
    let degraded = checks.iter().any(|check| check.status == "warn");

    let output = DoctorOutput {
        kind: "aitrace.doctor.v1",
        status: if failed {
            "fail"
        } else if degraded {
            "degraded"
        } else {
            "ok"
        },
        checks,
        supported_presets: vec![
            "overview",
            "cpu",
            "diagnostics",
            "energy",
            "hangs",
            "memory",
            "oslog",
        ],
        notes: vec![
            "stdout is reserved for structured results; xctrace noise is compacted into normalized errors".to_string(),
            "set AITRACE_CACHE_DIR to override the default macOS cache directory".to_string(),
        ],
    };

    output::print(format, &output)
}

fn ok(name: &'static str, detail: impl Into<String>) -> Check {
    Check {
        name,
        status: "ok",
        detail: detail.into(),
    }
}

fn warn(name: &'static str, detail: impl Into<String>) -> Check {
    Check {
        name,
        status: "warn",
        detail: detail.into(),
    }
}

fn fail(name: &'static str, detail: impl Into<String>) -> Check {
    Check {
        name,
        status: "fail",
        detail: detail.into(),
    }
}
