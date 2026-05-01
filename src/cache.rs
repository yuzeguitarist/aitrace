use crate::cli::Preset;
use crate::toc::SchemaRef;
use anyhow::{Context, Result};
use blake3::Hasher;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub const PARSER_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-perf3");

#[derive(Debug, Clone, Serialize)]
pub struct CacheInfo {
    pub trace_fingerprint: String,
    pub cache_dir: String,
    pub db_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexedRow {
    pub evidence_id: String,
    pub preset: String,
    pub run: String,
    pub table_index: usize,
    pub schema_name: String,
    pub row_index: usize,
    pub symbol_hint: Option<String>,
    pub module_hint: Option<String>,
    pub thread_hint: Option<String>,
    pub score: f64,
    pub percent_hint: Option<f64>,
    pub time_ms_hint: Option<f64>,
    pub flat_text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawFragment {
    pub evidence_id: String,
    pub kind: String,
    pub text: String,
}

pub fn cache_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("AITRACE_CACHE_DIR") {
        return PathBuf::from(path);
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join("Library")
            .join("Caches")
            .join("aitrace");
    }
    PathBuf::from(".aitrace-cache")
}

pub fn ensure_cache_dir() -> Result<PathBuf> {
    let dir = cache_dir();
    match fs::create_dir_all(&dir) {
        Ok(()) if cache_dir_is_writable(&dir) => Ok(dir),
        Ok(()) if std::env::var_os("AITRACE_CACHE_DIR").is_none() => {
            let fallback = std::env::temp_dir().join("aitrace-cache");
            fs::create_dir_all(&fallback).with_context(|| {
                format!(
                    "default cache {} is not writable; also failed fallback {}",
                    dir.display(),
                    fallback.display()
                )
            })?;
            Ok(fallback)
        }
        Ok(()) => Err(anyhow::anyhow!("cache directory is not writable"))
            .with_context(|| format!("failed to write test file in {}", dir.display())),
        Err(err) if std::env::var_os("AITRACE_CACHE_DIR").is_none() => {
            let fallback = std::env::temp_dir().join("aitrace-cache");
            fs::create_dir_all(&fallback).with_context(|| {
                format!(
                    "failed to create default cache {} ({err}); also failed fallback {}",
                    dir.display(),
                    fallback.display()
                )
            })?;
            Ok(fallback)
        }
        Err(err) => Err(err).with_context(|| format!("failed to create {}", dir.display())),
    }
}

fn cache_dir_is_writable(dir: &Path) -> bool {
    let probe = dir.join(format!(".aitrace-write-test-{}", std::process::id()));
    let result = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe)
        .and_then(|_| fs::remove_file(&probe));
    result.is_ok()
}

pub fn trace_fingerprint(trace: &Path, xctrace_version: &str) -> Result<String> {
    let mut hasher = Hasher::new();
    hasher.update(PARSER_VERSION.as_bytes());
    hasher.update(xctrace_version.as_bytes());
    fingerprint_path(trace, trace, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn db_path_for(trace: &Path, xctrace_version: &str) -> Result<CacheInfo> {
    let fingerprint = trace_fingerprint(trace, xctrace_version)?;
    let dir = ensure_cache_dir()?;
    let db_path = dir.join(format!("{}.db", fingerprint));
    Ok(CacheInfo {
        trace_fingerprint: fingerprint,
        cache_dir: dir.display().to_string(),
        db_path: db_path.display().to_string(),
    })
}

fn fingerprint_path(root: &Path, path: &Path, hasher: &mut Hasher) -> Result<()> {
    let meta = fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let relative = path.strip_prefix(root).unwrap_or(path);
    hasher.update(relative.to_string_lossy().as_bytes());
    hasher.update(&meta.len().to_le_bytes());
    if let Ok(modified) = meta.modified() {
        if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
            hasher.update(&duration.as_secs().to_le_bytes());
            hasher.update(&duration.subsec_nanos().to_le_bytes());
        }
    }

    if meta.is_file() {
        let mut file = fs::File::open(path)
            .with_context(|| format!("failed to open {} for fingerprinting", path.display()))?;
        let mut buf = [0u8; 64 * 1024];
        let mut remaining = 1024 * 1024usize;
        while remaining > 0 {
            let chunk_len = buf.len().min(remaining);
            let read_len = file.read(&mut buf[..chunk_len])?;
            if read_len == 0 {
                break;
            }
            hasher.update(&buf[..read_len]);
            remaining -= read_len;
        }
    } else if meta.is_dir() {
        let mut entries = fs::read_dir(path)?
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        entries.sort();
        for entry in entries {
            fingerprint_path(root, &entry, hasher)?;
        }
    }

    Ok(())
}

pub fn open_db(db_path: &Path) -> Result<Connection> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    init_db(&conn)?;
    Ok(conn)
}

fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS trace_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS schemas (
            run TEXT NOT NULL,
            table_index INTEGER NOT NULL,
            schema_name TEXT NOT NULL,
            name TEXT,
            documentation TEXT,
            xpath TEXT NOT NULL,
            PRIMARY KEY (run, table_index, schema_name)
        );

        CREATE TABLE IF NOT EXISTS raw_fragments (
            evidence_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            preset TEXT,
            run TEXT,
            table_index INTEGER,
            row_index INTEGER,
            schema_name TEXT,
            xpath TEXT,
            xml_zstd BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rows (
            evidence_id TEXT PRIMARY KEY,
            preset TEXT NOT NULL,
            run TEXT NOT NULL,
            table_index INTEGER NOT NULL,
            schema_name TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            symbol_hint TEXT,
            module_hint TEXT,
            thread_hint TEXT,
            score REAL NOT NULL,
            percent_hint REAL,
            time_ms_hint REAL,
            flat_text TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_rows_preset_score ON rows(preset, score DESC);
        CREATE INDEX IF NOT EXISTS idx_rows_schema ON rows(schema_name);
        CREATE INDEX IF NOT EXISTS idx_rows_table ON rows(run, table_index, row_index);
        "#,
    )?;
    Ok(())
}

pub fn set_meta(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO trace_meta(key, value) VALUES(?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}

pub fn get_meta(conn: &Connection, key: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT value FROM trace_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?)
}

pub fn clear_index(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        DELETE FROM schemas;
        DELETE FROM raw_fragments;
        DELETE FROM rows;
        "#,
    )?;
    Ok(())
}

pub fn insert_schema(conn: &Connection, schema: &SchemaRef) -> Result<()> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO schemas(run, table_index, schema_name, name, documentation, xpath)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            schema.run,
            schema.table_index as i64,
            schema.schema_name,
            schema.name,
            schema.documentation,
            schema.suggested_xpath
        ],
    )?;
    Ok(())
}

pub fn insert_raw_fragment(
    conn: &Connection,
    evidence_id: &str,
    kind: &str,
    preset: Option<Preset>,
    schema: &SchemaRef,
    row_index: Option<usize>,
    xml: &[u8],
) -> Result<()> {
    let compressed = zstd::stream::encode_all(xml, 6)?;
    conn.execute(
        r#"
        INSERT OR REPLACE INTO raw_fragments(
            evidence_id, kind, preset, run, table_index, row_index, schema_name, xpath, xml_zstd
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        "#,
        params![
            evidence_id,
            kind,
            preset.map(|p| p.as_str().to_string()),
            schema.run,
            schema.table_index as i64,
            row_index.map(|v| v as i64),
            schema.schema_name,
            schema.suggested_xpath,
            compressed
        ],
    )?;
    Ok(())
}

pub fn insert_row(conn: &Connection, row: &IndexedRow) -> Result<()> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO rows(
            evidence_id, preset, run, table_index, schema_name, row_index,
            symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
        "#,
        params![
            row.evidence_id,
            row.preset,
            row.run,
            row.table_index as i64,
            row.schema_name,
            row.row_index as i64,
            row.symbol_hint,
            row.module_hint,
            row.thread_hint,
            row.score,
            row.percent_hint,
            row.time_ms_hint,
            row.flat_text
        ],
    )?;
    Ok(())
}

pub fn load_top_rows(
    conn: &Connection,
    preset: Preset,
    run: Option<&str>,
    target: Option<&str>,
    thread: Option<&str>,
    hide_system: bool,
    limit: usize,
) -> Result<Vec<IndexedRow>> {
    let sql = if run.is_some() {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE preset = ?1 AND run = ?2
        ORDER BY score DESC, row_index ASC
        LIMIT 2000
        "#
    } else {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE preset = ?1
        ORDER BY score DESC, row_index ASC
        LIMIT 2000
        "#
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = if let Some(run) = run {
        stmt.query_map(params![preset.as_str(), run], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        stmt.query_map(params![preset.as_str()], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    };
    let target_lc = target.map(|v| v.to_ascii_lowercase());
    let thread_lc = thread.map(|v| v.to_ascii_lowercase());
    let mut out = Vec::new();

    for row in rows {
        let text_lc = row.flat_text.to_ascii_lowercase();
        if let Some(target_lc) = &target_lc {
            let symbol = row
                .symbol_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            let module = row
                .module_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            if !text_lc.contains(target_lc)
                && !symbol.contains(target_lc)
                && !module.contains(target_lc)
            {
                continue;
            }
        }
        if let Some(thread_lc) = &thread_lc {
            let thread_hint = row
                .thread_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            if !text_lc.contains(thread_lc) && !thread_hint.contains(thread_lc) {
                continue;
            }
        }
        if hide_system && is_obvious_system_row(&row) {
            continue;
        }
        out.push(row);
        if out.len() >= limit {
            break;
        }
    }

    Ok(out)
}

pub fn search_rows(
    conn: &Connection,
    preset: Option<Preset>,
    terms: &BTreeMap<&str, String>,
    limit: usize,
) -> Result<Vec<IndexedRow>> {
    let sql = if preset.is_some() {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE preset = ?1
        ORDER BY score DESC, row_index ASC
        LIMIT 5000
        "#
    } else {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        ORDER BY score DESC, row_index ASC
        LIMIT 5000
        "#
    };
    let mut stmt = conn.prepare(sql)?;
    let rows: Vec<IndexedRow> = if let Some(preset) = preset {
        stmt.query_map(params![preset.as_str()], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        stmt.query_map([], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    };

    let lowered_terms = terms
        .iter()
        .map(|(k, v)| (*k, v.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for row in rows {
        let text = row.flat_text.to_ascii_lowercase();
        let symbol = row
            .symbol_hint
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        let module = row
            .module_hint
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        let thread = row
            .thread_hint
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        let matched = lowered_terms.iter().all(|(kind, value)| match *kind {
            "symbol" => text.contains(value) || symbol.contains(value),
            "module" => text.contains(value) || module.contains(value),
            "thread" => text.contains(value) || thread.contains(value),
            "text" => text.contains(value),
            _ => text.contains(value),
        });
        if matched {
            out.push(row);
            if out.len() >= limit {
                break;
            }
        }
    }
    Ok(out)
}

pub fn load_rows_for_preset(
    conn: &Connection,
    preset: Preset,
    run: Option<&str>,
    target: Option<&str>,
    thread: Option<&str>,
    hide_system: bool,
    max_rows: usize,
) -> Result<Vec<IndexedRow>> {
    let sql = if run.is_some() {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE preset = ?1 AND run = ?2
        ORDER BY row_index ASC
        LIMIT ?3
        "#
    } else {
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE preset = ?1
        ORDER BY row_index ASC
        LIMIT ?2
        "#
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = if let Some(run) = run {
        stmt.query_map(params![preset.as_str(), run, max_rows as i64], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        stmt.query_map(params![preset.as_str(), max_rows as i64], row_from_sql)?
            .collect::<Result<Vec<_>, _>>()?
    };
    let target_lc = target.map(|v| v.to_ascii_lowercase());
    let thread_lc = thread.map(|v| v.to_ascii_lowercase());
    let mut out = Vec::new();

    for row in rows {
        let text_lc = row.flat_text.to_ascii_lowercase();
        if let Some(target_lc) = &target_lc {
            let symbol = row
                .symbol_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            let module = row
                .module_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            if !text_lc.contains(target_lc)
                && !symbol.contains(target_lc)
                && !module.contains(target_lc)
            {
                continue;
            }
        }
        if let Some(thread_lc) = &thread_lc {
            let thread_hint = row
                .thread_hint
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase();
            if !text_lc.contains(thread_lc) && !thread_hint.contains(thread_lc) {
                continue;
            }
        }
        if hide_system && is_obvious_system_row(&row) {
            continue;
        }
        out.push(row);
    }

    Ok(out)
}

pub fn row_by_evidence(conn: &Connection, evidence_id: &str) -> Result<Option<IndexedRow>> {
    conn.query_row(
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows WHERE evidence_id = ?1
        "#,
        params![evidence_id],
        row_from_sql,
    )
    .optional()
    .map_err(Into::into)
}

pub fn neighboring_rows(
    conn: &Connection,
    row: &IndexedRow,
    depth: usize,
) -> Result<Vec<IndexedRow>> {
    let start = row.row_index.saturating_sub(depth);
    let end = row.row_index + depth;
    let mut stmt = conn.prepare(
        r#"
        SELECT evidence_id, preset, run, table_index, schema_name, row_index,
               symbol_hint, module_hint, thread_hint, score, percent_hint, time_ms_hint, flat_text
        FROM rows
        WHERE run = ?1 AND table_index = ?2 AND row_index BETWEEN ?3 AND ?4
        ORDER BY row_index ASC
        "#,
    )?;
    let rows = stmt.query_map(
        params![row.run, row.table_index as i64, start as i64, end as i64],
        row_from_sql,
    )?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub fn raw_fragment(conn: &Connection, evidence_id: &str) -> Result<Option<RawFragment>> {
    let fragment: Option<(String, Vec<u8>)> = conn
        .query_row(
            "SELECT kind, xml_zstd FROM raw_fragments WHERE evidence_id = ?1",
            params![evidence_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    if let Some((kind, compressed)) = fragment {
        let bytes = zstd::stream::decode_all(&compressed[..])?;
        Ok(Some(RawFragment {
            evidence_id: evidence_id.to_string(),
            kind,
            text: String::from_utf8_lossy(&bytes).into_owned(),
        }))
    } else {
        Ok(None)
    }
}

pub fn indexed_preset_exists(conn: &Connection, preset: Preset) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM rows WHERE preset = ?1",
        params![preset.as_str()],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn indexed_preset_run_exists(conn: &Connection, preset: Preset, run: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM rows WHERE preset = ?1 AND run = ?2",
        params![preset.as_str(), run],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn latest_indexed_run(conn: &Connection, preset: Option<Preset>) -> Result<Option<String>> {
    let sql = if preset.is_some() {
        r#"
        SELECT run FROM rows
        WHERE preset = ?1
        GROUP BY run
        ORDER BY CAST(run AS INTEGER) DESC, run DESC
        LIMIT 1
        "#
    } else {
        r#"
        SELECT run FROM rows
        GROUP BY run
        ORDER BY CAST(run AS INTEGER) DESC, run DESC
        LIMIT 1
        "#
    };
    let mut stmt = conn.prepare(sql)?;
    let run = if let Some(preset) = preset {
        stmt.query_row(params![preset.as_str()], |row| row.get(0))
            .optional()?
    } else {
        stmt.query_row([], |row| row.get(0)).optional()?
    };
    Ok(run)
}

pub fn row_count(conn: &Connection, preset: Option<Preset>) -> Result<usize> {
    let count: i64 = if let Some(preset) = preset {
        conn.query_row(
            "SELECT COUNT(*) FROM rows WHERE preset = ?1",
            params![preset.as_str()],
            |row| row.get(0),
        )?
    } else {
        conn.query_row("SELECT COUNT(*) FROM rows", [], |row| row.get(0))?
    };
    Ok(count as usize)
}

pub fn row_count_scoped(
    conn: &Connection,
    preset: Option<Preset>,
    run: Option<&str>,
) -> Result<usize> {
    let count: i64 = match (preset, run) {
        (Some(preset), Some(run)) => conn.query_row(
            "SELECT COUNT(*) FROM rows WHERE preset = ?1 AND run = ?2",
            params![preset.as_str(), run],
            |row| row.get(0),
        )?,
        (Some(preset), None) => conn.query_row(
            "SELECT COUNT(*) FROM rows WHERE preset = ?1",
            params![preset.as_str()],
            |row| row.get(0),
        )?,
        (None, Some(run)) => conn.query_row(
            "SELECT COUNT(*) FROM rows WHERE run = ?1",
            params![run],
            |row| row.get(0),
        )?,
        (None, None) => conn.query_row("SELECT COUNT(*) FROM rows", [], |row| row.get(0))?,
    };
    Ok(count as usize)
}

fn row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexedRow> {
    Ok(IndexedRow {
        evidence_id: row.get(0)?,
        preset: row.get(1)?,
        run: row.get(2)?,
        table_index: row.get::<_, i64>(3)? as usize,
        schema_name: row.get(4)?,
        row_index: row.get::<_, i64>(5)? as usize,
        symbol_hint: row.get(6)?,
        module_hint: row.get(7)?,
        thread_hint: row.get(8)?,
        score: row.get(9)?,
        percent_hint: row.get(10)?,
        time_ms_hint: row.get(11)?,
        flat_text: row.get(12)?,
    })
}

fn is_obvious_system_row(row: &IndexedRow) -> bool {
    let joined = format!(
        "{} {} {}",
        row.symbol_hint.as_deref().unwrap_or(""),
        row.module_hint.as_deref().unwrap_or(""),
        row.flat_text
    )
    .to_ascii_lowercase();

    const NEEDLES: &[&str] = &[
        "/system/library/",
        "/usr/lib/",
        "corefoundation",
        "foundation.framework",
        "swiftui.framework",
        "libsystem_",
        "libobjc",
        "dyld",
        "quartzcore",
        "appkit.framework",
        "com.apple.nseventthread",
    ];

    NEEDLES.iter().any(|needle| joined.contains(needle))
}
