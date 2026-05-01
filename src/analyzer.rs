use crate::cache::{self, IndexedRow};
use crate::cli::{OutputFormat, Preset};
use crate::output;
use crate::toc::{self, SchemaRef};
use crate::xctrace;
use anyhow::{bail, Context, Result};
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::Reader;
use regex::Regex;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const RAW_TABLE_STORE_LIMIT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize)]
struct InspectOutput {
    kind: &'static str,
    trace: String,
    runs: Vec<crate::toc::RunRef>,
    schemas: Vec<SchemaRef>,
    available_views: Vec<String>,
    recommended_next: Vec<String>,
}

#[derive(Debug, Serialize)]
struct IndexOutput {
    kind: &'static str,
    trace: String,
    cache: cache::CacheInfo,
    selected_presets: Vec<String>,
    exported_tables: Vec<ExportedTable>,
    indexed_rows: usize,
    warnings: Vec<String>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ExportedTable {
    preset: String,
    run: String,
    table_index: usize,
    schema_name: String,
    evidence: String,
    rows_indexed: usize,
    bytes: usize,
}

#[derive(Debug, Serialize)]
struct SummaryOutput {
    kind: &'static str,
    trace: String,
    preset: String,
    run: Option<String>,
    cache_db: String,
    target: Option<String>,
    thread: Option<String>,
    coverage: Coverage,
    findings: Vec<Finding>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DiagnoseOutput {
    v: &'static str,
    trace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    rows: usize,
    mix: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rca: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    main: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    swift: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    src: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    ev: Vec<String>,
    next: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Coverage {
    indexed_rows_for_preset: usize,
    shown_findings: usize,
    budget_chars: usize,
    hidden_system_frames: &'static str,
    note: String,
}

#[derive(Debug, Serialize, Clone)]
struct Finding {
    id: String,
    severity: &'static str,
    title: String,
    symbol: Option<String>,
    module: Option<String>,
    thread: Option<String>,
    score: f64,
    percent_hint: Option<f64>,
    time_ms_hint: Option<f64>,
    schema: String,
    evidence: String,
    excerpt: String,
    next: String,
}

#[derive(Debug, Serialize)]
struct FindOutput {
    kind: &'static str,
    trace: String,
    query: BTreeMap<String, String>,
    matches: Vec<Finding>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DrillOutput {
    kind: &'static str,
    trace: String,
    id: String,
    resolved_evidence: String,
    focus: Option<Finding>,
    nearby_evidence: Vec<RowExcerpt>,
    interpretation_hints: Vec<String>,
    next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RawOutput {
    kind: &'static str,
    trace: String,
    evidence: String,
    raw_kind: Option<String>,
    text: String,
    nearby: Vec<RowExcerpt>,
}

#[derive(Debug, Serialize)]
struct RowExcerpt {
    evidence: String,
    row_index: usize,
    score: f64,
    excerpt: String,
}

#[derive(Debug)]
struct ParsedRow {
    row_index: usize,
    flat_text: String,
}

pub fn inspect(trace: PathBuf, format: OutputFormat, limit: usize) -> Result<()> {
    validate_trace(&trace)?;
    let toc_xml = xctrace::export_toc(&trace)?;
    let mut toc = toc::parse_toc(&toc_xml)?;
    toc.schemas.truncate(limit);
    let available_views = available_views(&toc.schemas);
    let trace_s = trace.display().to_string();
    let mut recommended_next = available_views
        .iter()
        .take(4)
        .map(|view| {
            let budget = if view == "energy" || view == "diagnostics" {
                1600
            } else {
                1200
            };
            format!(
                "aitrace summary {} --preset {} --budget {}",
                shell_quote(&trace),
                view,
                budget
            )
        })
        .collect::<Vec<_>>();
    recommended_next.push(format!("aitrace index {}", shell_quote(&trace)));
    let output = InspectOutput {
        kind: "aitrace.inspect.v1",
        trace: trace_s.clone(),
        runs: toc.runs,
        schemas: toc.schemas,
        available_views,
        recommended_next,
    };
    output::print(format, &output)
}

pub fn export(trace: PathBuf, xpath: String) -> Result<()> {
    validate_trace(&trace)?;
    let xml = xctrace::export_xpath(&trace, &xpath)?;
    print!("{}", String::from_utf8_lossy(&xml));
    Ok(())
}

pub fn index(
    trace: PathBuf,
    preset: Option<Preset>,
    run: Option<String>,
    force: bool,
    format: OutputFormat,
    limit_rows_per_table: usize,
) -> Result<()> {
    validate_trace(&trace)?;
    let xctrace_version = xctrace::xctrace_version().unwrap_or_else(|_| "unknown".to_string());
    let cache_info = cache::db_path_for(&trace, &xctrace_version)?;
    let db_path = PathBuf::from(&cache_info.db_path);
    let conn = cache::open_db(&db_path)?;

    if !force && cache::get_meta(&conn, "complete")?.as_deref() == Some("true") {
        let selected = selected_presets(preset);
        let existing = selected.iter().all(|p| match run.as_deref() {
            Some(run)
                if !run.eq_ignore_ascii_case("all") && !run.eq_ignore_ascii_case("latest") =>
            {
                cache::indexed_preset_run_exists(&conn, *p, run).unwrap_or(false)
            }
            _ => cache::indexed_preset_exists(&conn, *p).unwrap_or(false),
        });
        if existing {
            let row_count = cache::row_count(&conn, None)?;
            let output = IndexOutput {
                kind: "aitrace.index.v1",
                trace: trace.display().to_string(),
                cache: cache_info,
                selected_presets: selected.iter().map(|p| p.as_str().to_string()).collect(),
                exported_tables: Vec::new(),
                indexed_rows: row_count,
                warnings: vec!["cache already exists; use --force to rebuild".to_string()],
                next_commands: default_next_commands(&trace),
            };
            return output::print(format, &output);
        }
    }

    let output = build_index(
        &trace,
        &conn,
        preset,
        run.as_deref(),
        None,
        &cache_info,
        limit_rows_per_table,
        force,
    )?;
    output::print(format, &output)
}

#[allow(clippy::too_many_arguments)]
pub fn summary(
    trace: PathBuf,
    preset: Preset,
    run: Option<String>,
    target: Option<String>,
    thread: Option<String>,
    budget: usize,
    limit: usize,
    hide_system: bool,
    no_auto_index: bool,
    format: OutputFormat,
) -> Result<()> {
    let (conn, cache_info, resolved_run) =
        ensure_index_for(&trace, Some(preset), no_auto_index, run.as_deref())?;
    let findings = if preset == Preset::Cpu {
        let rows = cache::load_rows_for_preset(
            &conn,
            preset,
            resolved_run.as_deref(),
            target.as_deref(),
            thread.as_deref(),
            hide_system,
            250_000,
        )?;
        cpu_findings_from_rows(&trace, rows, limit, budget / limit.max(1).max(2))
    } else {
        let rows = cache::load_top_rows(
            &conn,
            preset,
            resolved_run.as_deref(),
            target.as_deref(),
            thread.as_deref(),
            hide_system,
            limit,
        )?;
        findings_from_rows(preset, &trace, rows, budget / limit.max(1).max(2))
    };
    let indexed_rows = cache::row_count_scoped(&conn, Some(preset), resolved_run.as_deref())?;
    let output = SummaryOutput {
        kind: "aitrace.summary.v1",
        trace: trace.display().to_string(),
        preset: preset.as_str().to_string(),
        run: resolved_run,
        cache_db: cache_info.db_path,
        target,
        thread,
        coverage: Coverage {
            indexed_rows_for_preset: indexed_rows,
            shown_findings: findings.len(),
            budget_chars: budget,
            hidden_system_frames: if hide_system { "folded" } else { "shown" },
            note: "v0.1 uses schema-aware table selection plus generic XML row scoring; raw evidence remains available through evidence IDs".to_string(),
        },
        next_commands: findings
            .iter()
            .take(3)
            .map(|f| {
                format!(
                    "aitrace drill {} {} --budget 1000",
                    shell_quote(&trace),
                    f.evidence
                )
            })
            .collect(),
        findings,
    };
    output::print(format, &output)
}

#[allow(clippy::too_many_arguments)]
pub fn diagnose(
    trace: PathBuf,
    target: Option<String>,
    run: String,
    repo: Option<PathBuf>,
    budget: usize,
    limit: usize,
    hide_system: bool,
    no_auto_index: bool,
    format: OutputFormat,
) -> Result<()> {
    let run_filter = if run.eq_ignore_ascii_case("all") {
        None
    } else {
        Some(run.as_str())
    };
    let (conn, _, resolved_run) =
        ensure_index_for(&trace, Some(Preset::Cpu), no_auto_index, run_filter)?;
    let rows = cache::load_rows_for_preset(
        &conn,
        Preset::Cpu,
        resolved_run.as_deref(),
        target.as_deref(),
        None,
        hide_system,
        250_000,
    )?;
    let mix_rows = if hide_system {
        cache::load_rows_for_preset(
            &conn,
            Preset::Cpu,
            resolved_run.as_deref(),
            target.as_deref(),
            None,
            false,
            250_000,
        )?
    } else {
        rows.clone()
    };
    let indexed_rows = cache::row_count_scoped(&conn, Some(Preset::Cpu), resolved_run.as_deref())?;
    let aggs = cpu_aggs_from_rows(rows.iter());
    let top_aggs = aggs.iter().take(limit.max(1)).collect::<Vec<_>>();
    let main_aggs = aggs
        .iter()
        .filter(|agg| is_main_thread(agg.thread.as_deref(), &agg.first_excerpt))
        .take(limit.max(1))
        .collect::<Vec<_>>();
    let mut source_hints = source_hints(repo.as_deref(), &top_aggs, &main_aggs)?;

    source_hints.sort();
    source_hints.dedup();
    source_hints.truncate(6);

    let top = top_aggs
        .iter()
        .map(|agg| compact_cpu_line(agg, repo.as_deref(), false))
        .collect::<Result<Vec<_>>>()?;
    let main = main_aggs
        .iter()
        .map(|agg| compact_cpu_line(agg, repo.as_deref(), true))
        .collect::<Result<Vec<_>>>()?;
    let swift = swift_signal_lines(&aggs, limit.clamp(2, 6));
    let rca = rca_lines(&aggs, &rows, repo.as_deref(), target.as_deref())?;
    let mut ev = Vec::new();
    for line in rca.iter().chain(top.iter()).chain(main.iter()) {
        if let Some(evidence) = evidence_from_line(line) {
            if !ev.contains(&evidence) {
                ev.push(evidence);
            }
        }
        if ev.len() >= 4 {
            break;
        }
    }
    let next = ev
        .first()
        .map(|evidence| {
            vec![format!(
                "aitrace drill {} {} --budget 900",
                shell_quote(&trace),
                evidence
            )]
        })
        .unwrap_or_else(|| {
            vec![format!(
                "aitrace summary {} --preset cpu --run {} --budget 1200",
                shell_quote(&trace),
                run
            )]
        });

    let trace_label = trace
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToString::to_string)
        .unwrap_or_else(|| trace.display().to_string());

    let mut output = DiagnoseOutput {
        v: "aitrace.diagnose.v1",
        trace: trace_label,
        run: resolved_run,
        target,
        rows: indexed_rows,
        mix: cpu_mix_line(&mix_rows),
        rca,
        top,
        main,
        swift,
        src: source_hints,
        ev,
        next,
    };
    trim_diagnose_to_budget(&mut output, budget);
    output::print(format, &output)
}

#[allow(clippy::too_many_arguments)]
pub fn find(
    trace: PathBuf,
    preset: Option<Preset>,
    symbol: Option<String>,
    module: Option<String>,
    thread: Option<String>,
    regex: Option<String>,
    budget: usize,
    limit: usize,
    no_auto_index: bool,
    format: OutputFormat,
) -> Result<()> {
    let search_preset = if symbol.is_some() && preset.is_none() {
        Some(Preset::Cpu)
    } else {
        preset
    };
    let (conn, _, _) = ensure_index_for(&trace, search_preset, no_auto_index, None)?;
    let mut terms_for_db = BTreeMap::new();
    if let Some(symbol) = &symbol {
        terms_for_db.insert("symbol", symbol.clone());
    }
    if let Some(module) = &module {
        terms_for_db.insert("module", module.clone());
    }
    if let Some(thread) = &thread {
        terms_for_db.insert("thread", thread.clone());
    }

    let mut rows = if symbol.is_some() {
        cache::load_rows_for_preset(
            &conn,
            search_preset.unwrap_or(Preset::Cpu),
            None,
            None,
            thread.as_deref(),
            false,
            250_000,
        )?
    } else if terms_for_db.is_empty() && regex.is_none() {
        cache::load_top_rows(
            &conn,
            search_preset.unwrap_or(Preset::Cpu),
            None,
            None,
            None,
            false,
            5000,
        )?
    } else {
        cache::search_rows(&conn, search_preset, &terms_for_db, 5000)?
    };

    if let Some(symbol) = &symbol {
        rows.retain(|row| symbol_query_matches(row, symbol));
    }
    if let Some(module) = &module {
        let module = module.to_ascii_lowercase();
        rows.retain(|row| {
            row.module_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase()
                .contains(&module)
                || row.flat_text.to_ascii_lowercase().contains(&module)
        });
    }

    if let Some(pattern) = &regex {
        let re = Regex::new(pattern).with_context(|| format!("invalid regex: {pattern}"))?;
        rows.retain(|row| re.is_match(&row.flat_text));
    }
    rows.truncate(limit);

    let query = [
        ("preset", preset.map(|p| p.as_str().to_string())),
        ("symbol", symbol),
        ("module", module),
        ("thread", thread),
        ("regex", regex),
    ]
    .into_iter()
    .filter_map(|(key, value)| value.map(|value| (key.to_string(), value)))
    .collect::<BTreeMap<_, _>>();

    let findings = findings_from_rows(
        search_preset.unwrap_or(Preset::Overview),
        &trace,
        rows,
        budget / limit.max(1).max(2),
    );
    let output = FindOutput {
        kind: "aitrace.find.v1",
        trace: trace.display().to_string(),
        query,
        next_commands: findings
            .iter()
            .take(5)
            .map(|f| {
                format!(
                    "aitrace drill {} {} --budget 1000",
                    shell_quote(&trace),
                    f.evidence
                )
            })
            .collect(),
        matches: findings,
    };
    output::print(format, &output)
}

pub fn drill(
    trace: PathBuf,
    id: String,
    preset: Preset,
    depth: usize,
    budget: usize,
    no_auto_index: bool,
    format: OutputFormat,
) -> Result<()> {
    let (conn, _, _) = ensure_index_for(&trace, Some(preset), no_auto_index, None)?;
    let evidence = if id.starts_with("ev:") {
        id.clone()
    } else {
        resolve_finding_id(&conn, preset, &trace, &id)?
    };

    let row = cache::row_by_evidence(&conn, &evidence)?;
    let (focus, nearby_evidence) = if let Some(row) = row {
        let focus = finding_from_row(preset, &trace, &row, 1, budget / 2);
        let nearby = cache::neighboring_rows(&conn, &row, depth)?
            .into_iter()
            .map(|row| RowExcerpt {
                evidence: row.evidence_id,
                row_index: row.row_index,
                score: row.score,
                excerpt: output::truncate_chars(&row.flat_text, 220),
            })
            .collect();
        (Some(focus), nearby)
    } else {
        (None, Vec::new())
    };

    let hints = focus.as_ref().map(interpretation_hints).unwrap_or_else(|| {
        vec![
            "No indexed row matched; try a raw table evidence ID or rebuild with a broader preset"
                .to_string(),
        ]
    });

    let output = DrillOutput {
        kind: "aitrace.drill.v1",
        trace: trace.display().to_string(),
        id,
        resolved_evidence: evidence.clone(),
        focus,
        nearby_evidence,
        interpretation_hints: hints,
        next_commands: vec![format!(
            "aitrace raw {} {} --context {} --budget 4000",
            shell_quote(&trace),
            evidence,
            depth
        )],
    };
    output::print(format, &output)
}

pub fn raw(
    trace: PathBuf,
    evidence: String,
    context: usize,
    budget: usize,
    no_auto_index: bool,
    format: OutputFormat,
) -> Result<()> {
    let (conn, _, _) = ensure_index_for(&trace, None, no_auto_index, None)?;
    let raw = cache::raw_fragment(&conn, &evidence)?;
    let row = cache::row_by_evidence(&conn, &evidence)?;
    let nearby = if context > 0 {
        if let Some(row) = row.as_ref() {
            cache::neighboring_rows(&conn, row, context)?
                .into_iter()
                .map(|row| RowExcerpt {
                    evidence: row.evidence_id,
                    row_index: row.row_index,
                    score: row.score,
                    excerpt: output::truncate_chars(&row.flat_text, 220),
                })
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let text = if let Some(raw) = raw.as_ref() {
        output::truncate_chars(&raw.text, budget)
    } else if let Some(row) = row {
        output::truncate_chars(&row.flat_text, budget)
    } else {
        bail!("evidence not found in index: {evidence}");
    };

    let output = RawOutput {
        kind: "aitrace.raw.v1",
        trace: trace.display().to_string(),
        evidence,
        raw_kind: raw.map(|raw| raw.kind),
        text,
        nearby,
    };
    output::print(format, &output)
}

#[allow(clippy::too_many_arguments)]
fn build_index(
    trace: &Path,
    conn: &Connection,
    preset: Option<Preset>,
    run_filter: Option<&str>,
    toc_override: Option<toc::Toc>,
    cache_info: &cache::CacheInfo,
    limit_rows_per_table: usize,
    force: bool,
) -> Result<IndexOutput> {
    if force {
        cache::clear_index(conn)?;
    }

    let toc = if let Some(toc) = toc_override {
        toc
    } else {
        let toc_xml = xctrace::export_toc(trace)?;
        toc::parse_toc(&toc_xml)?
    };
    let resolved_run = resolve_run_filter(&toc, run_filter)?;
    with_transaction(conn, |conn| {
        cache::set_meta(conn, "parser_version", cache::PARSER_VERSION)?;
        cache::set_meta(conn, "trace_fingerprint", &cache_info.trace_fingerprint)?;
        cache::set_meta(conn, "complete", "false")?;
        for schema in &toc.schemas {
            cache::insert_schema(conn, schema)?;
        }
        Ok(())
    })?;

    let selected = selected_presets(preset);
    let mut warnings = Vec::new();
    let mut exported_tables = Vec::new();

    for selected_preset in selected.iter().copied() {
        let matched = matching_schemas(&toc.schemas, selected_preset);
        let matched = matched
            .into_iter()
            .filter(|schema| {
                resolved_run
                    .as_ref()
                    .map(|run| schema.run == *run)
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        if matched.is_empty() {
            warnings.push(format!(
                "no table matched preset '{}'",
                selected_preset.as_str()
            ));
            continue;
        }

        for schema in matched {
            let table_evidence = table_evidence_id(selected_preset, schema);
            match xctrace::export_xpath(trace, &schema.suggested_xpath) {
                Ok(xml) => {
                    let rows = parse_rows(&xml, limit_rows_per_table)?;
                    let mut rows_indexed = 0usize;
                    with_transaction(conn, |conn| {
                        if xml.len() <= RAW_TABLE_STORE_LIMIT_BYTES {
                            cache::insert_raw_fragment(
                                conn,
                                &table_evidence,
                                "table",
                                Some(selected_preset),
                                schema,
                                None,
                                &xml,
                            )?;
                        } else {
                            warnings.push(format!(
                                "raw table omitted for {} run {} table {} ({} bytes > {} bytes); indexed row evidence remains available",
                                schema.schema_name,
                                schema.run,
                                schema.table_index,
                                xml.len(),
                                RAW_TABLE_STORE_LIMIT_BYTES
                            ));
                        }

                        for row in rows {
                            let evidence_id =
                                row_evidence_id(selected_preset, schema, row.row_index);
                            let indexed = indexed_row_from_parsed(
                                selected_preset,
                                schema,
                                row.row_index,
                                evidence_id,
                                row.flat_text,
                            );
                            cache::insert_row(conn, &indexed)?;
                            rows_indexed += 1;
                        }
                        Ok(())
                    })?;
                    exported_tables.push(ExportedTable {
                        preset: selected_preset.as_str().to_string(),
                        run: schema.run.clone(),
                        table_index: schema.table_index,
                        schema_name: schema.schema_name.clone(),
                        evidence: table_evidence,
                        rows_indexed,
                        bytes: xml.len(),
                    });
                }
                Err(err) => warnings.push(format!(
                    "failed to export {} table {} ({}): {}",
                    schema.run, schema.table_index, schema.schema_name, err
                )),
            }
        }
    }

    with_transaction(conn, |conn| cache::set_meta(conn, "complete", "true"))?;
    let indexed_rows = cache::row_count(conn, None)?;
    Ok(IndexOutput {
        kind: "aitrace.index.v1",
        trace: trace.display().to_string(),
        cache: cache_info.clone(),
        selected_presets: selected.iter().map(|p| p.as_str().to_string()).collect(),
        exported_tables,
        indexed_rows,
        warnings,
        next_commands: default_next_commands(trace),
    })
}

fn with_transaction<T>(conn: &Connection, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    match f(conn) {
        Ok(value) => {
            conn.execute_batch("COMMIT")?;
            Ok(value)
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

fn ensure_index_for(
    trace: &Path,
    preset: Option<Preset>,
    no_auto_index: bool,
    run_filter: Option<&str>,
) -> Result<(Connection, cache::CacheInfo, Option<String>)> {
    validate_trace(trace)?;
    let xctrace_version = xctrace::xctrace_version().unwrap_or_else(|_| "unknown".to_string());
    let cache_info = cache::db_path_for(trace, &xctrace_version)?;
    let db_path = PathBuf::from(&cache_info.db_path);
    let conn = cache::open_db(&db_path)?;
    let mut toc_for_build = None;
    let resolved_run = if matches!(run_filter, Some("latest")) {
        if let Some(run) = cache::latest_indexed_run(&conn, preset)? {
            Some(run)
        } else if no_auto_index {
            Some("latest".to_string())
        } else {
            let toc_xml = xctrace::export_toc(trace)?;
            let toc = toc::parse_toc(&toc_xml)?;
            let resolved = resolve_run_filter(&toc, run_filter)?;
            toc_for_build = Some(toc);
            resolved
        }
    } else {
        run_filter
            .filter(|run| !run.eq_ignore_ascii_case("all"))
            .map(ToString::to_string)
    };
    let needs_index = match preset {
        Some(preset) => {
            if let Some(run) = resolved_run.as_deref() {
                !cache::indexed_preset_run_exists(&conn, preset, run)?
            } else {
                !cache::indexed_preset_exists(&conn, preset)?
            }
        }
        None => cache::row_count(&conn, None)? == 0,
    };
    if needs_index {
        if no_auto_index {
            bail!(
                "index is missing for {}; run: aitrace index {}",
                preset.map(|p| p.as_str()).unwrap_or("all presets"),
                shell_quote(trace)
            );
        }
        let _ = build_index(
            trace,
            &conn,
            preset,
            resolved_run.as_deref(),
            toc_for_build,
            &cache_info,
            200_000,
            false,
        )?;
    }
    Ok((conn, cache_info, resolved_run))
}

fn resolve_run_filter(toc: &toc::Toc, run_filter: Option<&str>) -> Result<Option<String>> {
    match run_filter {
        None => Ok(None),
        Some(run) if run.eq_ignore_ascii_case("all") => Ok(None),
        Some(run) if run.eq_ignore_ascii_case("latest") => toc
            .runs
            .last()
            .map(|run| Some(run.number.clone()))
            .context("trace has no runs"),
        Some(run) => Ok(Some(run.to_string())),
    }
}

fn selected_presets(preset: Option<Preset>) -> Vec<Preset> {
    match preset {
        Some(Preset::Overview) | None => Preset::all_index_presets().to_vec(),
        Some(preset) => vec![preset],
    }
}

fn matching_schemas(schemas: &[SchemaRef], preset: Preset) -> Vec<&SchemaRef> {
    if preset == Preset::Cpu {
        let primary = schemas
            .iter()
            .filter(|schema| {
                let name = schema.schema_name.to_ascii_lowercase();
                name == "cpu-profile" || name == "time-profile"
            })
            .collect::<Vec<_>>();
        if !primary.is_empty() {
            return primary;
        }

        return schemas
            .iter()
            .filter(|schema| schema.schema_name.eq_ignore_ascii_case("time-sample"))
            .collect();
    }

    schemas
        .iter()
        .filter(|schema| {
            toc::schema_matches(
                &schema.schema_name,
                schema.name.as_deref(),
                preset.schema_needles(),
            )
        })
        .collect()
}

fn available_views(schemas: &[SchemaRef]) -> Vec<String> {
    let mut views = BTreeSet::new();
    for preset in Preset::all_index_presets() {
        if !matching_schemas(schemas, *preset).is_empty() {
            views.insert(preset.as_str().to_string());
        }
    }
    views.into_iter().collect()
}

fn parse_rows(xml: &[u8], limit: usize) -> Result<Vec<ParsedRow>> {
    let mut reader = Reader::from_reader(Cursor::new(xml));
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut rows = Vec::new();

    let mut in_row = false;
    let mut row_depth = 0usize;
    let mut current_raw = Vec::<u8>::new();
    let mut current_text = Vec::<String>::new();
    let mut row_index = 0usize;
    let mut refs = HashMap::<String, String>::new();
    let mut element_stack = Vec::<String>::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"row" && !in_row => {
                in_row = true;
                row_depth = 1;
                row_index += 1;
                current_raw.clear();
                current_text.clear();
                element_stack.clear();
                element_stack.push("row".to_string());
                append_start(&mut current_raw, &e, false);
                append_attrs_text(&mut current_text, &e, &mut refs);
            }
            Event::Start(e) if in_row => {
                row_depth += 1;
                element_stack.push(String::from_utf8_lossy(e.name().as_ref()).into_owned());
                append_start(&mut current_raw, &e, false);
                append_attrs_text(&mut current_text, &e, &mut refs);
            }
            Event::Empty(e) if in_row => {
                append_start(&mut current_raw, &e, true);
                append_attrs_text(&mut current_text, &e, &mut refs);
            }
            Event::Text(e) if in_row => {
                current_raw.extend_from_slice(e.as_ref());
                let text = String::from_utf8_lossy(e.as_ref()).trim().to_string();
                if !text.is_empty() {
                    let element = element_stack.last().map(String::as_str).unwrap_or("");
                    if element.is_empty() || element == "row" {
                        current_text.push(text);
                    } else {
                        current_text.push(format!("{element}={text}"));
                    }
                }
            }
            Event::CData(e) if in_row => {
                current_raw.extend_from_slice(b"<![CDATA[");
                current_raw.extend_from_slice(e.as_ref());
                current_raw.extend_from_slice(b"]]>");
                let text = String::from_utf8_lossy(e.as_ref()).trim().to_string();
                if !text.is_empty() {
                    let element = element_stack.last().map(String::as_str).unwrap_or("");
                    if element.is_empty() || element == "row" {
                        current_text.push(text);
                    } else {
                        current_text.push(format!("{element}={text}"));
                    }
                }
            }
            Event::End(e) if in_row => {
                append_end(&mut current_raw, &e);
                element_stack.pop();
                row_depth = row_depth.saturating_sub(1);
                if row_depth == 0 {
                    in_row = false;
                    rows.push(ParsedRow {
                        row_index,
                        flat_text: output::one_line(&current_text.join(" ")),
                    });
                    if rows.len() >= limit {
                        break;
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(rows)
}

fn append_attrs_text(
    parts: &mut Vec<String>,
    e: &BytesStart<'_>,
    refs: &mut HashMap<String, String>,
) {
    let element = String::from_utf8_lossy(e.name().as_ref()).into_owned();
    let attrs = e
        .attributes()
        .with_checks(false)
        .flatten()
        .map(|attr| {
            (
                String::from_utf8_lossy(attr.key.as_ref()).into_owned(),
                String::from_utf8_lossy(attr.value.as_ref()).into_owned(),
            )
        })
        .collect::<Vec<_>>();

    if let Some((_, ref_id)) = attrs.iter().find(|(key, _)| key == "ref") {
        if let Some(value) = refs.get(ref_id) {
            parts.push(value.clone());
            return;
        }
    }

    let id = attrs
        .iter()
        .find(|(key, _)| key == "id")
        .map(|(_, value)| value.clone());
    if let Some((_, fmt)) = attrs.iter().find(|(key, _)| key == "fmt") {
        if !fmt.trim().is_empty() {
            let token = format!("{element}={fmt}");
            parts.push(token.clone());
            if let Some(id) = id {
                refs.insert(id, token);
            }
        }
    }

    for (key, value) in attrs {
        if !value.trim().is_empty() {
            match key.as_ref() {
                "fmt" => {}
                "name" | "documentation" | "schema" | "mnemonic" => {
                    parts.push(format!("{key}={value}"));
                }
                "id" | "ref" => {}
                _ => parts.push(format!("{key}={value}")),
            }
        }
    }
}

fn append_start(out: &mut Vec<u8>, e: &BytesStart<'_>, empty: bool) {
    out.push(b'<');
    out.extend_from_slice(e.name().as_ref());
    for attr in e.attributes().with_checks(false).flatten() {
        out.push(b' ');
        out.extend_from_slice(attr.key.as_ref());
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(attr.value.as_ref());
        out.push(b'"');
    }
    if empty {
        out.extend_from_slice(b"/>");
    } else {
        out.push(b'>');
    }
}

fn append_end(out: &mut Vec<u8>, e: &BytesEnd<'_>) {
    out.extend_from_slice(b"</");
    out.extend_from_slice(e.name().as_ref());
    out.push(b'>');
}

fn indexed_row_from_parsed(
    preset: Preset,
    schema: &SchemaRef,
    row_index: usize,
    evidence_id: String,
    flat_text: String,
) -> IndexedRow {
    let percent_hint = max_percent(&flat_text);
    let time_ms_hint = max_time_ms(&flat_text);
    let score = score_row(percent_hint, time_ms_hint, row_index);
    IndexedRow {
        evidence_id,
        preset: preset.as_str().to_string(),
        run: schema.run.clone(),
        table_index: schema.table_index,
        schema_name: schema.schema_name.clone(),
        row_index,
        symbol_hint: if preset == Preset::Energy {
            process_hint(&flat_text).or_else(|| symbol_hint(&flat_text))
        } else {
            symbol_hint(&flat_text).map(|symbol| normalize_swift_symbol(&symbol))
        },
        module_hint: if preset == Preset::Energy {
            pid_hint(&flat_text).or_else(|| module_hint(&flat_text))
        } else {
            module_hint(&flat_text)
        },
        thread_hint: thread_hint(&flat_text),
        score,
        percent_hint,
        time_ms_hint,
        flat_text,
    }
}

fn findings_from_rows(
    preset: Preset,
    trace: &Path,
    rows: Vec<IndexedRow>,
    excerpt_budget: usize,
) -> Vec<Finding> {
    rows.iter()
        .enumerate()
        .map(|(idx, row)| finding_from_row(preset, trace, row, idx + 1, excerpt_budget))
        .collect()
}

#[derive(Debug, Clone)]
struct CpuAgg {
    symbol: String,
    module: Option<String>,
    thread: Option<String>,
    total_weight: f64,
    unit: &'static str,
    sample_count: usize,
    first_evidence: String,
    first_schema: String,
    first_excerpt: String,
}

fn cpu_findings_from_rows(
    trace: &Path,
    rows: Vec<IndexedRow>,
    limit: usize,
    excerpt_budget: usize,
) -> Vec<Finding> {
    cpu_aggs_from_rows(rows.iter())
        .into_iter()
        .take(limit)
        .enumerate()
        .map(|(idx, row)| Finding {
            id: format!("cpu.hotspot.{}", idx + 1),
            severity: if row.total_weight >= high_cpu_threshold(row.unit) {
                "high"
            } else if row.total_weight >= medium_cpu_threshold(row.unit) {
                "medium"
            } else {
                "low"
            },
            title: row.symbol.clone(),
            symbol: Some(row.symbol),
            module: row.module,
            thread: row.thread,
            score: row.total_weight,
            percent_hint: None,
            time_ms_hint: if row.unit == "ms" {
                Some(row.total_weight)
            } else {
                None
            },
            schema: row.first_schema,
            evidence: row.first_evidence.clone(),
            excerpt: output::truncate_chars(
                &format!(
                    "cpu={} samples={} rep={}",
                    format_cpu_weight(row.total_weight, row.unit),
                    row.sample_count,
                    row.first_excerpt
                ),
                excerpt_budget.max(140),
            ),
            next: format!(
                "aitrace drill {} {} --depth 8 --budget 1000",
                shell_quote(trace),
                row.first_evidence
            ),
        })
        .collect()
}

fn cpu_aggs_from_rows<'a>(rows: impl Iterator<Item = &'a IndexedRow>) -> Vec<CpuAgg> {
    let mut agg = HashMap::<String, CpuAgg>::new();

    for row in rows {
        let (weight, unit) = row_cpu_weight(row);
        if weight <= 0.0 {
            continue;
        }
        let symbols = cpu_symbols_for_row(row);
        if symbols.is_empty() {
            continue;
        }
        let thread = row.thread_hint.clone();
        for symbol in symbols {
            let key = format!("{}|{}", symbol, thread.as_deref().unwrap_or(""));
            let entry = agg.entry(key).or_insert_with(|| CpuAgg {
                symbol,
                module: row.module_hint.clone(),
                thread: thread.clone(),
                total_weight: 0.0,
                unit,
                sample_count: 0,
                first_evidence: row.evidence_id.clone(),
                first_schema: row.schema_name.clone(),
                first_excerpt: row.flat_text.clone(),
            });
            entry.total_weight += weight;
            entry.sample_count += 1;
        }
    }

    let mut rows = agg.into_values().collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        b.total_weight
            .total_cmp(&a.total_weight)
            .then(b.sample_count.cmp(&a.sample_count))
    });

    rows
}

fn is_low_signal_symbol(symbol: &str) -> bool {
    let lower = symbol.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "running" | "blocked" | "waiting" | "runnable" | "unknown" | "corenlp"
    ) || is_token_noise_symbol(symbol)
        || lower.contains("mach_msg")
        || lower.contains("semaphore_wait")
        || lower.starts_with("0x")
}

fn row_cpu_weight(row: &IndexedRow) -> (f64, &'static str) {
    if let Some(ms) = row.time_ms_hint {
        return (ms, "ms");
    }
    if let Some(cycles) = cycle_weight_hint(&row.flat_text) {
        return (cycles, "cy");
    }
    (1.0, "sample")
}

fn cycle_weight_hint(text: &str) -> Option<f64> {
    static WITH_UNIT: OnceLock<Regex> = OnceLock::new();
    let with_unit = WITH_UNIT
        .get_or_init(|| Regex::new(r"(?i)\bcycle-weight=(\d+(?:\.\d+)?)\s*([kmg])\b").unwrap());
    if let Some(cap) = with_unit.captures(text) {
        let value = cap.get(1)?.as_str().parse::<f64>().ok()?;
        let multiplier = match &cap.get(2)?.as_str().to_ascii_lowercase()[..] {
            "k" => 1_000.0,
            "m" => 1_000_000.0,
            "g" => 1_000_000_000.0,
            _ => 1.0,
        };
        return Some(value * multiplier);
    }

    static RAW: OnceLock<Regex> = OnceLock::new();
    let raw = RAW.get_or_init(|| Regex::new(r"\bcycle-weight=(\d{2,})\b").unwrap());
    raw.captures(text)
        .and_then(|cap| cap.get(1)?.as_str().parse::<f64>().ok())
}

fn format_cpu_weight(value: f64, unit: &str) -> String {
    match unit {
        "ms" => format!("{value:.0}ms"),
        "cy" if value >= 1_000_000_000.0 => format!("{:.1}Gcy", value / 1_000_000_000.0),
        "cy" if value >= 1_000_000.0 => format!("{:.1}Mcy", value / 1_000_000.0),
        "cy" if value >= 1_000.0 => format!("{:.1}Kcy", value / 1_000.0),
        "cy" => format!("{value:.0}cy"),
        "sample" => format!("{value:.0}samp"),
        _ => format!("{value:.0}{unit}"),
    }
}

fn high_cpu_threshold(unit: &str) -> f64 {
    match unit {
        "ms" => 1000.0,
        "cy" => 1_000_000_000.0,
        _ => 1000.0,
    }
}

fn medium_cpu_threshold(unit: &str) -> f64 {
    match unit {
        "ms" => 100.0,
        "cy" => 100_000_000.0,
        _ => 100.0,
    }
}

fn cpu_symbols_for_row(row: &IndexedRow) -> Vec<String> {
    let mut candidates = symbols_from_text(&row.flat_text);
    if let Some(symbol) = &row.symbol_hint {
        candidates.insert(0, normalize_swift_symbol(symbol));
    }

    let mut seen = BTreeSet::new();
    let mut preferred = Vec::new();
    let mut fallback = Vec::new();
    for symbol in candidates {
        let symbol = normalize_swift_symbol(&html_unescape(&symbol));
        if !plausible_symbol(&symbol)
            || is_low_signal_symbol(&symbol)
            || !seen.insert(symbol.clone())
        {
            continue;
        }
        if is_rca_relevant_symbol(&symbol, &row.flat_text) {
            preferred.push(symbol);
        } else if !is_system_text(&symbol) {
            fallback.push(symbol);
        }
    }

    if preferred.is_empty() {
        fallback.truncate(2);
        fallback
    } else {
        preferred.truncate(6);
        preferred
    }
}

fn symbols_from_text(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(pos) = rest.find("name=") {
        let after = &rest[pos + "name=".len()..];
        let end = next_attr_offset(after).unwrap_or(after.len()).min(260);
        let value = clean_token(&after[..end]);
        if !value.is_empty() {
            out.push(value);
        }
        if end >= after.len() {
            break;
        }
        rest = &after[end..];
    }
    out
}

fn next_attr_offset(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut idx = 1usize;
    while idx + 3 < bytes.len() {
        if bytes[idx].is_ascii_whitespace() {
            let mut key_end = idx + 1;
            while key_end < bytes.len()
                && (bytes[key_end].is_ascii_alphanumeric()
                    || bytes[key_end] == b'-'
                    || bytes[key_end] == b'_')
            {
                key_end += 1;
            }
            if key_end > idx + 1 && key_end < bytes.len() && bytes[key_end] == b'=' {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
}

const RCA_ML_TOKENS: &[&str] = &[
    "embedding",
    "vector",
    "encode",
    "encoder",
    "tokenizer",
    "nlp",
    "mlmodel",
    "coreml",
    "corenlp",
    "inference",
];

const RCA_ML_SIGNAL_TOKENS: &[&str] = &[
    "embedding",
    "vector",
    "encode",
    "encoder",
    "tokenizer",
    "mlmodel",
    "coreml",
    "inference",
];

const RCA_SEARCH_TOKENS: &[&str] = &["search", "index", "backfill", "migration"];

fn is_rca_relevant_symbol(symbol: &str, _text: &str) -> bool {
    let lower = symbol.to_ascii_lowercase();
    contains_any(&lower, RCA_ML_TOKENS) || contains_any(&lower, RCA_SEARCH_TOKENS)
}

fn html_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
}

fn compact_cpu_line(agg: &CpuAgg, repo: Option<&Path>, force_main: bool) -> Result<String> {
    let thread = if force_main {
        "main".to_string()
    } else {
        short_thread(agg.thread.as_deref(), &agg.first_excerpt)
    };
    let mut line = format!(
        "{} {} {} ev={}",
        format_cpu_weight(agg.total_weight, agg.unit),
        thread,
        short_symbol(&agg.symbol, 86),
        agg.first_evidence
    );
    if let Some(src) = source_hint_for_symbol(repo, &agg.symbol)? {
        line.push_str(" src=");
        line.push_str(&src);
    }
    Ok(output::truncate_chars(&line, 260))
}

fn rca_lines(
    aggs: &[CpuAgg],
    rows: &[IndexedRow],
    repo: Option<&Path>,
    target: Option<&str>,
) -> Result<Vec<String>> {
    if aggs.is_empty() {
        return Ok(vec![
            "no_cpu_rows: CPU/time-profile rows not indexed; run inspect or try diagnostics/hangs"
                .to_string(),
        ]);
    }

    let mut out = Vec::new();
    push_rca_signal(
        &mut out,
        aggs,
        repo,
        target,
        "P0 main_ml",
        RCA_ML_SIGNAL_TOKENS,
        3,
    )?;
    push_rca_signal(
        &mut out,
        aggs,
        repo,
        target,
        "P0 main_search_index",
        RCA_SEARCH_TOKENS,
        2,
    )?;

    if let Some(main) = aggs
        .iter()
        .find(|agg| is_main_thread(agg.thread.as_deref(), &agg.first_excerpt))
    {
        let src = source_hint_for_symbol(repo, &main.symbol)?
            .map(|src| format!(" src={src}"))
            .unwrap_or_default();
        out.push(output::truncate_chars(
            &format!(
                "P0 main_cpu {} {} ev={}{}",
                format_cpu_weight(main.total_weight, main.unit),
                short_symbol(&main.symbol, 120),
                main.first_evidence,
                src
            ),
            240,
        ));
    }

    if let Some(actor) = aggs.iter().find(|agg| {
        is_main_thread(agg.thread.as_deref(), &agg.first_excerpt)
            && swift_async_actor_signal(&format!("{} {}", agg.symbol, agg.first_excerpt)).is_some()
    }) {
        out.push(output::truncate_chars(
            &format!(
                "P1 actor_async_main signal={} {} ev={}",
                swift_async_actor_signal(&format!("{} {}", actor.symbol, actor.first_excerpt))
                    .unwrap_or("swift"),
                short_symbol(&actor.symbol, 120),
                actor.first_evidence
            ),
            220,
        ));
    }

    if out.is_empty() {
        let top = &aggs[0];
        out.push(output::truncate_chars(
            &format!(
                "P1 top_cpu {} {} th={} ev={}",
                format_cpu_weight(top.total_weight, top.unit),
                short_symbol(&top.symbol, 120),
                short_thread(top.thread.as_deref(), &top.first_excerpt),
                top.first_evidence
            ),
            220,
        ));
    }

    if rows
        .iter()
        .any(|row| row.flat_text.to_ascii_lowercase().contains("mainactor"))
    {
        out.push(
            "hint: MainActor appears in CPU rows; verify isolation boundary before moving work"
                .to_string(),
        );
    }

    out.truncate(4);
    Ok(out)
}

fn push_rca_signal(
    out: &mut Vec<String>,
    aggs: &[CpuAgg],
    repo: Option<&Path>,
    target: Option<&str>,
    label: &str,
    tokens: &[&str],
    max_matches: usize,
) -> Result<()> {
    let mut pushed = 0usize;
    let mut matches = aggs
        .iter()
        .filter(|agg| {
            let text = rca_signal_text(agg);
            is_main_thread(agg.thread.as_deref(), &agg.first_excerpt)
                && has_app_context(agg, target)
                && is_heavy_cpu_signal(agg)
                && contains_any(&text, tokens)
        })
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| {
        rca_candidate_score(b, tokens)
            .total_cmp(&rca_candidate_score(a, tokens))
            .then(b.total_weight.total_cmp(&a.total_weight))
            .then(b.sample_count.cmp(&a.sample_count))
    });

    for agg in matches {
        if pushed >= max_matches.max(1) || out.len() >= 4 {
            break;
        }
        if out.iter().any(|line| line.contains(&agg.first_evidence)) {
            continue;
        }
        let src = source_hint_for_agg(repo, agg)?
            .map(|src| format!(" src={src}"))
            .unwrap_or_default();
        out.push(output::truncate_chars(
            &format!(
                "{} {} ev={} {}{}",
                label,
                format_cpu_weight(agg.total_weight, agg.unit),
                agg.first_evidence,
                compact_symbol_chain(&agg.first_excerpt, &agg.symbol),
                src
            ),
            260,
        ));
        pushed += 1;
    }
    Ok(())
}

fn rca_signal_text(agg: &CpuAgg) -> String {
    let mut parts = vec![symbol_token_text(&agg.symbol)];
    parts.extend(
        symbols_from_text(&agg.first_excerpt)
            .into_iter()
            .map(|symbol| symbol_token_text(&symbol)),
    );
    parts.join(" ").to_ascii_lowercase()
}

fn symbol_token_text(symbol: &str) -> String {
    let normalized = normalize_swift_symbol(&html_unescape(symbol));
    if is_token_noise_symbol(&normalized) {
        return String::new();
    }
    normalized
        .split('(')
        .next()
        .unwrap_or(&normalized)
        .to_string()
}

fn rca_candidate_score(agg: &CpuAgg, tokens: &[&str]) -> f64 {
    let mut score = normalized_cpu_weight(agg);
    if is_app_frame_symbol(&agg.symbol) {
        score += 2_000_000_000_000.0;
    } else if has_app_symbol_frame(agg) {
        score += 1_000_000_000_000.0;
    }
    if contains_any(&symbol_token_text(&agg.symbol).to_ascii_lowercase(), tokens) {
        score += 100_000_000_000.0;
    }
    score
}

fn normalized_cpu_weight(agg: &CpuAgg) -> f64 {
    match agg.unit {
        "ms" => agg.total_weight * 1_000_000.0,
        "cy" => agg.total_weight,
        _ => agg.sample_count as f64,
    }
}

fn has_app_symbol_frame(agg: &CpuAgg) -> bool {
    is_app_frame_symbol(&agg.symbol)
        || symbols_from_text(&agg.first_excerpt)
            .iter()
            .any(|symbol| is_app_frame_symbol(symbol))
}

fn is_app_frame_symbol(symbol: &str) -> bool {
    let normalized = normalize_swift_symbol(&html_unescape(symbol));
    let lower = normalized.to_ascii_lowercase();
    source_function_name(&normalized).is_some()
        && !is_token_noise_symbol(&normalized)
        && !normalized.contains("::")
        && !lower.starts_with("-[")
        && !is_system_text(&normalized)
        && !contains_any(
            &lower,
            &[
                "ag::",
                "bnns",
                "ca::",
                "cf",
                "corefoundation",
                "corenlp",
                "displaylist.",
                "foundation",
                "nlembedding",
                "objc_msgsend",
                "quartzcore",
                "sqlite3",
                "swift::",
                "swiftui",
            ],
        )
}

fn is_token_noise_symbol(symbol: &str) -> bool {
    let lower = symbol.to_ascii_lowercase();
    lower == "vector"
        || lower.starts_with("vector[abi")
        || lower.contains("std::vector")
        || lower.contains("std::__1::vector")
        || lower.contains("std::__1::basic_string")
}

fn is_heavy_cpu_signal(agg: &CpuAgg) -> bool {
    agg.total_weight >= rca_cpu_threshold(agg.unit)
}

fn rca_cpu_threshold(unit: &str) -> f64 {
    match unit {
        "ms" => 50.0,
        "cy" => 1_000_000.0,
        _ => 2.0,
    }
}

fn has_app_context(agg: &CpuAgg, target: Option<&str>) -> bool {
    let text = format!(
        "{} {} {}",
        agg.module.as_deref().unwrap_or(""),
        agg.thread.as_deref().unwrap_or(""),
        agg.first_excerpt
    );
    let lower = text.to_ascii_lowercase();
    if let Some(target) = target.map(str::trim).filter(|target| !target.is_empty()) {
        if lower.contains(&target.to_ascii_lowercase()) {
            return true;
        }
    }
    if agg
        .module
        .as_deref()
        .is_some_and(|module| !module.trim().is_empty() && !is_system_text(module))
    {
        return true;
    }
    process_hint(&agg.first_excerpt)
        .map(|process| {
            let process = process.to_ascii_lowercase();
            !process.trim().is_empty()
                && !contains_any(
                    &process,
                    &["kernel_task", "xctrace", "instruments", "com.apple."],
                )
        })
        .unwrap_or(false)
}

fn cpu_mix_line(rows: &[IndexedRow]) -> String {
    let mut total_weight = 0.0;
    let mut main_weight = 0.0;
    let mut system_weight = 0.0;
    let mut swift_weight = 0.0;
    let mut unit = "cy";

    for row in rows {
        let (weight, row_unit) = row_cpu_weight(row);
        if weight <= 0.0 {
            continue;
        }
        unit = row_unit;
        total_weight += weight;
        if is_main_thread(row.thread_hint.as_deref(), &row.flat_text) {
            main_weight += weight;
        }
        if is_system_text(row.symbol_hint.as_deref().unwrap_or("")) {
            system_weight += weight;
        }
        if swift_async_actor_signal(&row.flat_text).is_some() {
            swift_weight += weight;
        }
    }

    if total_weight <= 0.0 {
        return format!("cpu=0 rows={}", rows.len());
    }

    format!(
        "cpu={} main={:.0}% system={:.0}% swift_async={:.0}% rows={}",
        format_cpu_weight(total_weight, unit),
        main_weight * 100.0 / total_weight,
        system_weight * 100.0 / total_weight,
        swift_weight * 100.0 / total_weight,
        rows.len()
    )
}

fn swift_signal_lines(aggs: &[CpuAgg], limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    for agg in aggs {
        let text = format!("{} {}", agg.symbol, agg.first_excerpt);
        if let Some(signal) = swift_async_actor_signal(&text) {
            out.push(output::truncate_chars(
                &format!(
                    "{} {} {} th={} ev={}",
                    signal,
                    short_symbol(&agg.symbol, 96),
                    format_cpu_weight(agg.total_weight, agg.unit),
                    short_thread(agg.thread.as_deref(), &agg.first_excerpt),
                    agg.first_evidence
                ),
                180,
            ));
        }
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn trim_diagnose_to_budget(output: &mut DiagnoseOutput, budget: usize) {
    let floor = budget.max(700);
    while serde_yaml::to_string(output)
        .map(|text| text.chars().count() > floor)
        .unwrap_or(false)
    {
        if output.top.len() > 2 {
            output.top.pop();
        } else if output.main.len() > 2 {
            output.main.pop();
        } else if output.swift.len() > 1 {
            output.swift.pop();
        } else if output.src.len() > 1 {
            output.src.pop();
        } else if output.top.len() > 1 {
            output.top.pop();
        } else if output.main.len() > 1 {
            output.main.pop();
        } else if output.ev.len() > 3 {
            output.ev.pop();
        } else if output.rca.len() > 3 {
            output.rca.pop();
        } else {
            break;
        }
    }
}

fn evidence_from_line(line: &str) -> Option<String> {
    let start = line.find("ev=ev:")? + "ev=".len();
    let end = line[start..]
        .find(char::is_whitespace)
        .map(|offset| start + offset)
        .unwrap_or(line.len());
    Some(line[start..end].to_string())
}

fn finding_from_row(
    preset: Preset,
    trace: &Path,
    row: &IndexedRow,
    index: usize,
    excerpt_budget: usize,
) -> Finding {
    let title = if preset == Preset::Cpu {
        let chain = compact_symbol_chain(
            &row.flat_text,
            row.symbol_hint.as_deref().unwrap_or(&row.schema_name),
        );
        if chain.is_empty() {
            row.symbol_hint
                .clone()
                .or_else(|| row.module_hint.clone())
                .unwrap_or_else(|| format!("{} row {}", row.schema_name, row.row_index))
        } else {
            chain
        }
    } else {
        row.symbol_hint
            .clone()
            .or_else(|| row.module_hint.clone())
            .unwrap_or_else(|| format!("{} row {}", row.schema_name, row.row_index))
    };
    let (score, unit) = if preset == Preset::Cpu {
        row_cpu_weight(row)
    } else {
        (row.score, "score")
    };
    let excerpt = if preset == Preset::Cpu {
        format!("cpu={} {}", format_cpu_weight(score, unit), row.flat_text)
    } else {
        row.flat_text.clone()
    };
    Finding {
        id: format!("{}.hotspot.{}", preset.as_str(), index),
        severity: if preset == Preset::Cpu {
            if score >= high_cpu_threshold(unit) {
                "high"
            } else if score >= medium_cpu_threshold(unit) {
                "medium"
            } else {
                "low"
            }
        } else {
            severity(row)
        },
        title,
        symbol: row.symbol_hint.clone(),
        module: row.module_hint.clone(),
        thread: row.thread_hint.clone(),
        score,
        percent_hint: row.percent_hint,
        time_ms_hint: row.time_ms_hint,
        schema: row.schema_name.clone(),
        evidence: row.evidence_id.clone(),
        excerpt: output::truncate_chars(&excerpt, excerpt_budget.max(160)),
        next: format!(
            "aitrace drill {} {} --depth 8 --budget 1000",
            shell_quote(trace),
            row.evidence_id
        ),
    }
}

fn severity(row: &IndexedRow) -> &'static str {
    if row.percent_hint.unwrap_or(0.0) >= 40.0 || row.time_ms_hint.unwrap_or(0.0) >= 1000.0 {
        "high"
    } else if row.percent_hint.unwrap_or(0.0) >= 10.0 || row.time_ms_hint.unwrap_or(0.0) >= 100.0 {
        "medium"
    } else {
        "low"
    }
}

fn resolve_finding_id(conn: &Connection, preset: Preset, trace: &Path, id: &str) -> Result<String> {
    let suffix = id
        .rsplit('.')
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .context("finding ID must look like cpu.hotspot.1 or use ev:... evidence directly")?;
    let rows = cache::load_top_rows(conn, preset, None, None, None, true, suffix)?;
    let row = rows.get(suffix.saturating_sub(1)).with_context(|| {
        format!(
            "finding not found: {id}; try aitrace summary {}",
            shell_quote(trace)
        )
    })?;
    Ok(row.evidence_id.clone())
}

fn interpretation_hints(finding: &Finding) -> Vec<String> {
    let text = format!(
        "{} {} {}",
        finding.title,
        finding.module.as_deref().unwrap_or(""),
        finding.excerpt
    )
    .to_ascii_lowercase();
    let mut hints = Vec::new();
    if text.contains("main") {
        hints.push(
            "main-thread evidence: prioritize synchronous UI/search/IO work on this path"
                .to_string(),
        );
    }
    if text.contains("sqlite") || text.contains("fts") {
        hints.push("SQLite/FTS evidence: check query shape, indexes, row materialization, and debounce/cache behavior".to_string());
    }
    if text.contains("swiftui") || text.contains("layout") || text.contains("render") {
        hints.push("SwiftUI/render evidence: check update fanout, identity churn, animation frequency, and expensive body work".to_string());
    }
    if text.contains("alloc") || text.contains("vm:") || text.contains("imageio") {
        hints.push(
            "memory evidence: inspect retained/transient allocation classes before assuming a leak"
                .to_string(),
        );
    }
    if text.contains("fault") || text.contains("error") {
        hints.push("log evidence: correlate fault/error timestamps with CPU/hang rows before changing code".to_string());
    }
    if hints.is_empty() {
        hints.push("Use raw evidence only if this bounded excerpt is insufficient; avoid exporting or catting full XML in the AI loop".to_string());
    }
    hints
}

fn table_evidence_id(preset: Preset, schema: &SchemaRef) -> String {
    format!(
        "ev:{}:run{}:table{}",
        preset.as_str(),
        schema.run,
        schema.table_index
    )
}

fn row_evidence_id(preset: Preset, schema: &SchemaRef, row_index: usize) -> String {
    format!(
        "ev:{}:run{}:table{}:row{}",
        preset.as_str(),
        schema.run,
        schema.table_index,
        row_index
    )
}

fn validate_trace(trace: &Path) -> Result<()> {
    if !trace.exists() {
        bail!("trace path does not exist: {}", trace.display());
    }
    Ok(())
}

fn default_next_commands(trace: &Path) -> Vec<String> {
    vec![
        format!(
            "aitrace summary {} --preset cpu --target <AppName> --budget 1200",
            shell_quote(trace)
        ),
        format!(
            "aitrace drill {} <evidence-id> --depth 8 --budget 1000",
            shell_quote(trace)
        ),
        format!(
            "aitrace find {} --symbol <SymbolName> --budget 1200",
            shell_quote(trace)
        ),
    ]
}

fn shell_quote(path: &Path) -> String {
    let s = path.display().to_string();
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "/._-:+".contains(c))
    {
        s
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

fn max_percent(text: &str) -> Option<f64> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(?i)(\d+(?:\.\d+)?)\s*%").unwrap());
    re.captures_iter(text)
        .filter_map(|cap| cap.get(1)?.as_str().parse::<f64>().ok())
        .filter(|v| *v <= 100.0)
        .max_by(|a, b| a.total_cmp(b))
}

fn max_time_ms(text: &str) -> Option<f64> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r"(?ix)
            \b(?:duration(?:-[a-z]+)?|weight|cpu-time|cpu-total|duration-on-core|elapsed|latency|wall-time)
            =
            [^0-9]{0,16}
            (\d+(?:\.\d+)?)\s*(ns|µs|us|ms|s)\b
            ",
        )
        .unwrap()
    });
    re.captures_iter(text)
        .filter_map(|cap| {
            let value = cap.get(1)?.as_str().parse::<f64>().ok()?;
            let unit = cap.get(2)?.as_str().to_ascii_lowercase();
            Some(match unit.as_str() {
                "ns" => value / 1_000_000.0,
                "µs" | "us" => value / 1_000.0,
                "ms" => value,
                "s" => value * 1000.0,
                _ => value,
            })
        })
        .max_by(|a, b| a.total_cmp(b))
}

fn score_row(percent: Option<f64>, time_ms: Option<f64>, row_index: usize) -> f64 {
    let percent_score = percent.unwrap_or(0.0) * 10_000.0;
    let time_score = time_ms.unwrap_or(0.0);
    let stable_tiebreak = 1.0 / (row_index.max(1) as f64);
    percent_score + time_score + stable_tiebreak
}

fn symbol_hint(text: &str) -> Option<String> {
    static LABELED: OnceLock<Regex> = OnceLock::new();
    let labeled = LABELED.get_or_init(|| {
        Regex::new(r#"(?i)\b(symbol|function|name|label)=?["']?([A-Za-z_.$/@<>\-][A-Za-z0-9_.$:/@<>\-\+\[\]\(\)]+)"#).unwrap()
    });
    for cap in labeled.captures_iter(text) {
        if let Some(value) = cap.get(2).map(|m| clean_token(m.as_str())) {
            if plausible_symbol(&value) {
                return Some(value);
            }
        }
    }

    static TOKEN: OnceLock<Regex> = OnceLock::new();
    let token = TOKEN
        .get_or_init(|| Regex::new(r#"[A-Za-z_][A-Za-z0-9_.$:/@<>\-\+\[\]\(\)]{3,}"#).unwrap());
    let mut best = None::<String>;
    for m in token.find_iter(text) {
        let value = clean_token(m.as_str());
        if plausible_symbol(&value) {
            let replace = best
                .as_ref()
                .map(|current| value.len() > current.len())
                .unwrap_or(true);
            if replace {
                best = Some(value);
            }
        }
    }
    best
}

fn module_hint(text: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r#"(?i)\b(module|binary|library|image)=?["']?([A-Za-z0-9_.+\- ]{2,80})"#)
            .unwrap()
    });
    re.captures_iter(text)
        .filter_map(|cap| cap.get(2).map(|m| clean_token(m.as_str())))
        .next()
}

fn process_hint(text: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r#"\bprocess=([^=]+?)(?:\s+process=|\s+responsible-process=|\s+duration=|\s+pid=|\s+process-uid=|\s+system-cpu-percent=|\s+boolean=|\s+cpu-arch-name=|\s+event-count=)"#,
        )
        .unwrap()
    });
    re.captures(text)
        .and_then(|cap| cap.get(1).map(|m| clean_token(m.as_str())))
}

fn pid_hint(text: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r#"\bpid=([0-9]+)"#).unwrap());
    re.captures(text)
        .and_then(|cap| cap.get(1).map(|m| format!("pid {}", m.as_str())))
}

fn thread_hint(text: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r#"(?i)\b(thread|queue)=?["']?([A-Za-z0-9_.$:/@<>\- ]{2,80})"#).unwrap()
    });
    re.captures_iter(text)
        .filter_map(|cap| cap.get(2).map(|m| clean_token(m.as_str())))
        .next()
}

fn clean_token(token: &str) -> String {
    token
        .trim_matches(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ',' || c == ';')
        .to_string()
}

fn plausible_symbol(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if value.len() < 4 {
        return false;
    }
    if lower.starts_with("/system/")
        || lower.starts_with("/usr/lib/")
        || lower.starts_with("http")
        || lower.starts_with("schema")
        || lower.starts_with("row")
        || lower.starts_with("table")
    {
        return false;
    }
    value.contains('.')
        || value.contains("::")
        || value.contains('(')
        || value.contains('/')
        || value.chars().any(|c| c.is_ascii_uppercase())
}

fn normalize_swift_symbol(symbol: &str) -> String {
    let mut s = output::one_line(&clean_token(symbol));
    for prefix in [
        "specialized ",
        "merged ",
        "outlined ",
        "partial apply for ",
        "implicit closure #",
    ] {
        if s.to_ascii_lowercase().starts_with(prefix) {
            s = s[prefix.len()..].trim().to_string();
        }
    }

    let lower = s.to_ascii_lowercase();
    if (lower.contains("closure #")
        || lower.contains("thunk for")
        || lower.contains("reabstraction thunk"))
        && lower.contains(" in ")
    {
        if let Some(parent) = s.rsplit(" in ").next() {
            let parent = clean_token(parent);
            if parent.len() >= 4 {
                s = parent;
            }
        }
    }

    s = s
        .replace("function signature specialization <", "specialized <")
        .replace(" with unmangled suffix", "")
        .replace('`', "");
    strip_generic_args(&s)
}

fn symbol_query_matches(row: &IndexedRow, query: &str) -> bool {
    let query_lc = normalize_swift_symbol(query).to_ascii_lowercase();
    let compact_query = compact_symbol_key(&query_lc);
    let symbol = row
        .symbol_hint
        .as_deref()
        .map(normalize_swift_symbol)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let text = row.flat_text.to_ascii_lowercase();
    let normalized_text = normalize_swift_symbol(&row.flat_text).to_ascii_lowercase();
    let compact_symbol = compact_symbol_key(&symbol);
    let compact_text = compact_symbol_key(&normalized_text);

    symbol.contains(&query_lc)
        || text.contains(&query_lc)
        || normalized_text.contains(&query_lc)
        || (!compact_query.is_empty()
            && (compact_symbol.contains(&compact_query) || compact_text.contains(&compact_query)))
}

fn compact_symbol_key(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn strip_generic_args(symbol: &str) -> String {
    let mut out = String::new();
    let mut depth = 0usize;
    for c in symbol.chars() {
        match c {
            '<' => {
                if depth == 0 {
                    out.push_str("<_>");
                }
                depth += 1;
            }
            '>' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    output::one_line(&out)
}

fn compact_symbol_chain(text: &str, fallback: &str) -> String {
    let mut symbols = symbols_from_text(text)
        .into_iter()
        .map(|symbol| normalize_swift_symbol(&html_unescape(&symbol)))
        .filter(|symbol| plausible_symbol(symbol) && !is_low_signal_symbol(symbol))
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        symbols.push(normalize_swift_symbol(fallback));
    }

    let mut seen = BTreeSet::new();
    symbols.retain(|symbol| seen.insert(compact_symbol_key(symbol)));
    let relevant = symbols
        .iter()
        .filter(|symbol| is_rca_relevant_symbol(symbol, text))
        .cloned()
        .collect::<Vec<_>>();
    if !relevant.is_empty() {
        symbols = relevant;
        symbols.sort_by_key(|symbol| symbol_chain_rank(symbol));
    } else {
        symbols.retain(|symbol| !is_system_text(symbol));
    }
    symbols.truncate(4);
    symbols
        .into_iter()
        .map(|symbol| short_symbol(&symbol, 72))
        .collect::<Vec<_>>()
        .join("<-")
}

fn symbol_chain_rank(symbol: &str) -> u8 {
    if is_app_frame_symbol(symbol) {
        0
    } else if !is_system_text(symbol) {
        1
    } else {
        2
    }
}

fn short_symbol(symbol: &str, budget: usize) -> String {
    output::truncate_chars(&display_symbol(symbol), budget)
}

fn display_symbol(symbol: &str) -> String {
    let normalized = normalize_swift_symbol(symbol);
    let lower = normalized.to_ascii_lowercase();
    if lower.contains("corenlp::contextualwordembedding::fillwordvectors") {
        return "CoreNLP::ContextualWordEmbedding::fillWordVectors".to_string();
    }
    if lower.contains("corenlp::sentenceembedding::fillstringvector") {
        return "CoreNLP::SentenceEmbedding::fillStringVector".to_string();
    }
    if lower.contains("nlembedding") && lower.contains("sentenceembeddingforlanguage") {
        return "NLEmbedding.sentenceEmbeddingForLanguage".to_string();
    }
    normalized
}

fn short_thread(thread: Option<&str>, text: &str) -> String {
    if is_main_thread(thread, text) {
        return "main".to_string();
    }
    thread
        .map(|thread| {
            output::truncate_chars(
                &thread
                    .replace("com.apple.", "")
                    .replace("com.deck.", "")
                    .replace("Thread ", "T"),
                36,
            )
        })
        .unwrap_or_else(|| "thread?".to_string())
}

fn is_main_thread(thread: Option<&str>, text: &str) -> bool {
    let joined = format!("{} {}", thread.unwrap_or(""), text).to_ascii_lowercase();
    contains_any(
        &joined,
        &[
            "main thread",
            "main-thread",
            "com.apple.main-thread",
            "dispatchqueue.main",
            "thread=main",
            "queue=main",
        ],
    )
}

fn swift_async_actor_signal(text: &str) -> Option<&'static str> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("mainactor") {
        Some("mainactor")
    } else if lower.contains("swift_task") || lower.contains("async") {
        Some("async")
    } else if lower.contains("partial apply") || lower.contains("closure #") {
        Some("closure")
    } else if lower.contains("thunk") {
        Some("thunk")
    } else if lower.contains("dispatchqueue") || lower.contains("dispatch_async") {
        Some("dispatch")
    } else {
        None
    }
}

fn is_system_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    contains_any(
        &lower,
        &[
            "/system/library/",
            "/usr/lib/",
            "corefoundation",
            "foundation.framework",
            "swiftui.framework",
            "libsystem_",
            "libobjc",
            "quartzcore",
            "appkit.framework",
            "dyld",
            "com.apple.nseventthread",
        ],
    )
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn source_hints(repo: Option<&Path>, top: &[&CpuAgg], main: &[&CpuAgg]) -> Result<Vec<String>> {
    let Some(repo) = repo else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for agg in top.iter().chain(main.iter()) {
        if let Some(src) = source_hint_for_symbol(Some(repo), &agg.symbol)? {
            out.push(format!("{} {}", short_symbol(&agg.symbol, 72), src));
        }
        if out.len() >= 8 {
            break;
        }
    }
    Ok(out)
}

fn source_hint_for_symbol(repo: Option<&Path>, symbol: &str) -> Result<Option<String>> {
    let Some(repo) = repo else {
        return Ok(None);
    };
    let Some(name) = source_function_name(symbol) else {
        return Ok(None);
    };
    let terms = [
        format!("func {name}"),
        format!("static func {name}"),
        format!("class func {name}"),
        format!("var {name}"),
        format!("let {name}"),
    ];
    find_source_line(repo, &terms)
}

fn source_hint_for_agg(repo: Option<&Path>, agg: &CpuAgg) -> Result<Option<String>> {
    if let Some(src) = source_hint_for_symbol(repo, &agg.symbol)? {
        return Ok(Some(src));
    }
    for symbol in symbols_from_text(&agg.first_excerpt) {
        let symbol = normalize_swift_symbol(&html_unescape(&symbol));
        if let Some(src) = source_hint_for_symbol(repo, &symbol)? {
            return Ok(Some(src));
        }
    }
    Ok(None)
}

fn source_function_name(symbol: &str) -> Option<String> {
    let normalized = normalize_swift_symbol(symbol);
    if normalized.contains("::") {
        return None;
    }
    let before_params = normalized.split('(').next().unwrap_or(&normalized);
    let name = before_params
        .rsplit(['.', '/', ' '])
        .next()
        .unwrap_or(before_params)
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
    if name.len() >= 3 && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        Some(name.to_string())
    } else {
        None
    }
}

fn find_source_line(repo: &Path, terms: &[String]) -> Result<Option<String>> {
    let mut stack = vec![repo.to_path_buf()];
    let mut visited_files = 0usize;

    while let Some(path) = stack.pop() {
        if should_skip_path(&path) {
            continue;
        }
        let meta = match fs::metadata(&path) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if meta.is_dir() {
            let mut entries = match fs::read_dir(&path) {
                Ok(entries) => entries
                    .flatten()
                    .map(|entry| entry.path())
                    .collect::<Vec<_>>(),
                Err(_) => continue,
            };
            entries.sort();
            stack.extend(entries.into_iter().rev());
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("swift") || meta.len() > 2_000_000
        {
            continue;
        }
        visited_files += 1;
        if visited_files > 6000 {
            break;
        }

        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        for (idx, line) in content.lines().enumerate() {
            if terms.iter().any(|term| line.contains(term)) {
                let rel = path.strip_prefix(repo).unwrap_or(&path);
                let owner = owning_context(&content, idx).unwrap_or_default();
                let owner = if owner.is_empty() {
                    String::new()
                } else {
                    format!(" owner={owner}")
                };
                return Ok(Some(format!("{}:{}{}", rel.display(), idx + 1, owner)));
            }
        }
    }
    Ok(None)
}

fn owning_context(content: &str, line_index: usize) -> Option<String> {
    let lines = content.lines().take(line_index + 1).collect::<Vec<_>>();
    for line in lines.into_iter().rev().take(80) {
        let trimmed = line.trim();
        if trimmed.starts_with("func ")
            || trimmed.starts_with("static func ")
            || trimmed.starts_with("class func ")
            || trimmed.starts_with("actor ")
            || trimmed.starts_with("class ")
            || trimmed.starts_with("struct ")
            || trimmed.starts_with("extension ")
        {
            return Some(output::truncate_chars(trimmed, 80));
        }
    }
    None
}

fn should_skip_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    matches!(
        name,
        ".git"
            | ".build"
            | "build"
            | "DerivedData"
            | "node_modules"
            | "Pods"
            | "Carthage"
            | "target"
            | "xcuserdata"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rows_and_scores() {
        let xml = br#"
        <trace-query-result>
          <row id="1"><sample><symbol>ExampleDataStore.searchWithFTS</symbol><weight>4373.3 ms</weight><pct>68.1%</pct></sample></row>
          <row id="2"><sample><symbol>rowToClipboardItem</symbol><weight>1926.3 ms</weight><pct>30%</pct></sample></row>
        </trace-query-result>"#;
        let rows = parse_rows(xml, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].flat_text.contains("searchWithFTS"));
        assert_eq!(max_percent(&rows[0].flat_text), Some(68.1));
        assert_eq!(max_time_ms(&rows[0].flat_text), Some(4373.3));
    }

    #[test]
    fn extracts_symbol_hint() {
        let symbol = symbol_hint("symbol=ExampleDataStore.searchWithFTS 68.1%").unwrap();
        assert_eq!(symbol, "ExampleDataStore.searchWithFTS");
    }

    #[test]
    fn ignores_schema_only_exports() {
        let xml = br#"<trace-query-result><node><schema name="os-signpost"><col><name>Name</name></col></schema></node></trace-query-result>"#;
        let rows = parse_rows(xml, 10).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn extracts_energy_process_hint() {
        let xml = br#"<trace-query-result><node><row><process id="2" fmt="Deck (7990)"><pid id="3" fmt="7990">7990</pid></process><duration id="7" fmt="551.45 ms">551446625</duration><system-cpu-percent id="49" fmt="77.6%">77.581604352</system-cpu-percent></row></node></trace-query-result>"#;
        let row = parse_rows(xml, 10).unwrap().remove(0);
        assert!(row.flat_text.contains("process=Deck (7990)"));
        assert_eq!(
            process_hint(&row.flat_text),
            Some("Deck (7990)".to_string())
        );
        assert_eq!(max_percent(&row.flat_text), Some(77.6));
    }

    #[test]
    fn time_hint_ignores_addresses_and_prefers_duration_labels() {
        let text = "sample-time=00:12.733.679 thread=Deck addr=0x105f3545d weight=100.00 µs duration=1.50 ms";
        assert_eq!(max_time_ms(text), Some(1.5));
    }

    #[test]
    fn normalizes_swift_closure_to_parent_symbol() {
        let symbol = "partial apply for closure #1 in ExampleEmbeddingPipeline.makeVector(_:)";
        assert_eq!(
            normalize_swift_symbol(symbol),
            "ExampleEmbeddingPipeline.makeVector(_:)"
        );
    }

    #[test]
    fn symbol_query_matches_normalized_closure_text() {
        let row = IndexedRow {
            evidence_id: "ev:cpu:run1:table1:row1".to_string(),
            preset: "cpu".to_string(),
            run: "1".to_string(),
            table_index: 1,
            schema_name: "time-profile".to_string(),
            row_index: 1,
            symbol_hint: Some(
                "partial apply for closure #1 in ExampleEmbeddingPipeline.embeddingCache(_:)"
                    .to_string(),
            ),
            module_hint: Some("ExampleApp".to_string()),
            thread_hint: Some("Main Thread".to_string()),
            score: 1.0,
            percent_hint: None,
            time_ms_hint: Some(1.0),
            flat_text: "CoreNLP::ContextualWordEmbedding::fillWordVectors ExampleEmbeddingPipeline.embeddingCache".to_string(),
        };

        assert!(symbol_query_matches(&row, "embeddingCache"));
        assert!(symbol_query_matches(
            &row,
            "ExampleEmbeddingPipeline.embeddingCache"
        ));
    }
}
