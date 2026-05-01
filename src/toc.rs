use anyhow::Result;
use quick_xml::events::BytesStart;
use quick_xml::events::Event;
use quick_xml::Reader;
use serde::Serialize;
use std::borrow::Cow;
use std::io::Cursor;

#[derive(Debug, Clone, Serialize)]
pub struct RunRef {
    pub number: String,
    pub name: Option<String>,
    pub target_name: Option<String>,
    pub target_pid: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaRef {
    pub run: String,
    pub table_index: usize,
    pub schema_name: String,
    pub name: Option<String>,
    pub documentation: Option<String>,
    pub suggested_xpath: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Toc {
    pub runs: Vec<RunRef>,
    pub schemas: Vec<SchemaRef>,
}

#[derive(Debug, Clone)]
struct TableContext {
    index: usize,
    schema: Option<String>,
    name: Option<String>,
    documentation: Option<String>,
}

pub fn parse_toc(xml: &[u8]) -> Result<Toc> {
    let mut reader = Reader::from_reader(Cursor::new(xml));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut runs = Vec::new();
    let mut schemas = Vec::new();

    let mut current_run = "1".to_string();
    let mut table_index = 0usize;
    let mut table_stack: Vec<TableContext> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) if e.name().as_ref() == b"run" => {
                table_index = 0;
                current_run = attr(&e, b"number")
                    .or_else(|| attr(&e, b"id"))
                    .unwrap_or_else(|| (runs.len() + 1).to_string());
                runs.push(RunRef {
                    number: current_run.clone(),
                    name: attr(&e, b"name"),
                    target_name: attr(&e, b"target-name")
                        .or_else(|| attr(&e, b"target"))
                        .or_else(|| attr(&e, b"process")),
                    target_pid: attr(&e, b"target-pid").or_else(|| attr(&e, b"pid")),
                });
            }
            Event::Start(e) if e.name().as_ref() == b"table" => {
                table_index += 1;
                let idx = table_index;
                let schema = attr(&e, b"schema")
                    .or_else(|| attr(&e, b"schema-name"))
                    .or_else(|| attr(&e, b"name"));
                let name = attr(&e, b"name");
                let documentation = attr(&e, b"documentation").or_else(|| attr(&e, b"description"));
                if let Some(schema_name) = schema.clone() {
                    schemas.push(SchemaRef {
                        run: current_run.clone(),
                        table_index: idx,
                        schema_name,
                        name: name.clone(),
                        documentation: documentation.clone(),
                        suggested_xpath: xpath_for(&current_run, idx, schema.as_deref()),
                    });
                }
                table_stack.push(TableContext {
                    index: idx,
                    schema,
                    name,
                    documentation,
                });
            }
            Event::End(e) if e.name().as_ref() == b"table" => {
                table_stack.pop();
            }
            Event::Empty(e) if e.name().as_ref() == b"table" => {
                table_index += 1;
                let schema = attr(&e, b"schema")
                    .or_else(|| attr(&e, b"schema-name"))
                    .or_else(|| attr(&e, b"name"));
                if let Some(schema_name) = schema.clone() {
                    let idx = table_index;
                    schemas.push(SchemaRef {
                        run: current_run.clone(),
                        table_index: idx,
                        schema_name,
                        name: attr(&e, b"name"),
                        documentation: attr(&e, b"documentation")
                            .or_else(|| attr(&e, b"description")),
                        suggested_xpath: xpath_for(&current_run, idx, schema.as_deref()),
                    });
                }
            }
            Event::Start(e) | Event::Empty(e) if e.name().as_ref() == b"schema" => {
                let schema_name = attr(&e, b"name")
                    .or_else(|| attr(&e, b"id"))
                    .or_else(|| attr(&e, b"schema"));
                if let Some(schema_name) = schema_name {
                    let table_context = table_stack.last().cloned().unwrap_or(TableContext {
                        index: table_index.max(1),
                        schema: None,
                        name: None,
                        documentation: None,
                    });
                    if table_context.schema.as_deref() != Some(schema_name.as_str()) {
                        schemas.push(SchemaRef {
                            run: current_run.clone(),
                            table_index: table_context.index,
                            schema_name,
                            name: attr(&e, b"name").or(table_context.name),
                            documentation: attr(&e, b"documentation")
                                .or_else(|| attr(&e, b"description"))
                                .or(table_context.documentation),
                            suggested_xpath: xpath_for(
                                &current_run,
                                table_context.index,
                                table_context.schema.as_deref(),
                            ),
                        });
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if runs.is_empty() {
        runs.push(RunRef {
            number: "1".to_string(),
            name: None,
            target_name: None,
            target_pid: None,
        });
    }

    schemas.sort_by(|a, b| {
        a.run
            .cmp(&b.run)
            .then(a.table_index.cmp(&b.table_index))
            .then(a.schema_name.cmp(&b.schema_name))
    });
    schemas.dedup_by(|a, b| {
        a.run == b.run && a.table_index == b.table_index && a.schema_name == b.schema_name
    });

    Ok(Toc { runs, schemas })
}

pub fn xpath_for(run: &str, table_index: usize, schema: Option<&str>) -> String {
    if let Some(schema) = schema {
        if !schema.is_empty() {
            return format!(
                "/trace-toc/run[@number='{}']/data/table[@schema='{}']",
                escape_xpath(run),
                escape_xpath(schema)
            );
        }
    }

    format!(
        "/trace-toc/run[@number='{}']/data/table[{}]",
        escape_xpath(run),
        table_index
    )
}

fn escape_xpath(input: &str) -> Cow<'_, str> {
    if input.contains('\'') {
        Cow::Owned(input.replace('\'', "&apos;"))
    } else {
        Cow::Borrowed(input)
    }
}

fn attr(e: &BytesStart<'_>, key: &[u8]) -> Option<String> {
    for attr in e.attributes().with_checks(false).flatten() {
        if attr.key.as_ref() == key {
            return Some(String::from_utf8_lossy(attr.value.as_ref()).into_owned());
        }
    }
    None
}

pub fn schema_matches(schema_name: &str, name: Option<&str>, needles: &[&str]) -> bool {
    if needles.is_empty() {
        return true;
    }
    let haystack = format!(
        "{} {}",
        schema_name.to_ascii_lowercase(),
        name.unwrap_or("").to_ascii_lowercase()
    );
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_schema_toc() {
        let xml = br#"
        <trace-toc>
          <run number="1" name="Run 1">
            <data>
              <table schema="cpu-profile" name="CPU Profile"/>
              <table schema="os-log"/>
            </data>
          </run>
        </trace-toc>"#;
        let toc = parse_toc(xml).unwrap();
        assert_eq!(toc.schemas.len(), 2);
        assert_eq!(toc.schemas[0].schema_name, "cpu-profile");
        assert!(toc.schemas[0].suggested_xpath.contains("cpu-profile"));
    }
}
