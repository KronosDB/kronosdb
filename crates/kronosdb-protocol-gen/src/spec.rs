//! Emits the message reference that sits between the GENERATED markers in
//! `protocol/README.md`, so the prose spec can never disagree with the schema.

use std::fmt::Write;

use crate::schema::{Schema, Struct, Type, Width};

pub const BEGIN: &str = "<!-- BEGIN GENERATED: run `just protocol` to update -->";
pub const END: &str = "<!-- END GENERATED -->";

fn type_name(ty: &Type) -> String {
    let w = |w: &Width| w.bits();
    match ty {
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::I64 => "i64".into(),
        Type::Bool => "bool".into(),
        Type::F64 => "f64".into(),
        Type::Millis => "millis".into(),
        Type::Str(x) => format!("str{}", w(x)),
        Type::Bytes(x) => format!("bytes{}", w(x)),
        Type::List(x, inner) => format!("list{}<{}>", w(x), type_name(inner)),
        Type::Opt(inner) => format!("opt<{}>", type_name(inner)),
        Type::Record => "record".into(),
        Type::Records => "records".into(),
        Type::Named(n) => format!("[{n}](#{})", n.to_ascii_lowercase()),
    }
}

fn cell(lines: &[String]) -> String {
    lines.join(" ").replace('|', "\\|")
}

fn fields(o: &mut String, s: &Struct) {
    if s.fields.is_empty() {
        writeln!(o, "No fields.").unwrap();
        return;
    }
    writeln!(o, "| Field | Type | |").unwrap();
    writeln!(o, "|---|---|---|").unwrap();
    for f in &s.fields {
        writeln!(
            o,
            "| `{}` | {} | {} |",
            f.name,
            type_name(&f.ty),
            cell(&f.doc)
        )
        .unwrap();
    }
}

pub fn emit(schema: &Schema) -> String {
    let mut o = String::new();
    writeln!(o, "{BEGIN}").unwrap();
    writeln!(o).unwrap();
    writeln!(o, "### Frames").unwrap();
    writeln!(o).unwrap();
    writeln!(o, "| Kind | Frame | Sent by |").unwrap();
    writeln!(o, "|---|---|---|").unwrap();
    for f in schema.frames() {
        let by = if f.sent_by_server() {
            "server"
        } else {
            "client"
        };
        writeln!(
            o,
            "| `{:#06x}` | [{}](#{}) | {by} |",
            f.kind.unwrap(),
            f.name,
            f.name.to_ascii_lowercase()
        )
        .unwrap();
    }
    for f in schema.frames() {
        writeln!(o).unwrap();
        writeln!(o, "#### {}", f.name).unwrap();
        writeln!(o).unwrap();
        let by = if f.sent_by_server() {
            "server"
        } else {
            "client"
        };
        writeln!(o, "Kind `{:#06x}`, sent by the {by}.", f.kind.unwrap()).unwrap();
        if !f.doc.is_empty() {
            writeln!(o).unwrap();
            writeln!(o, "{}", f.doc.join("\n")).unwrap();
        }
        writeln!(o).unwrap();
        fields(&mut o, f);
    }
    writeln!(o).unwrap();
    writeln!(o, "### Structures").unwrap();
    for s in schema.structs.iter().filter(|s| s.kind.is_none()) {
        writeln!(o).unwrap();
        writeln!(o, "#### {}", s.name).unwrap();
        if !s.doc.is_empty() {
            writeln!(o).unwrap();
            writeln!(o, "{}", s.doc.join("\n")).unwrap();
        }
        writeln!(o).unwrap();
        fields(&mut o, s);
    }
    writeln!(o).unwrap();
    writeln!(o, "### Unions").unwrap();
    for u in &schema.unions {
        writeln!(o).unwrap();
        writeln!(o, "#### {}", u.name).unwrap();
        writeln!(o).unwrap();
        writeln!(
            o,
            "A `u8` tag, then the variant's value. {}",
            u.doc.join(" ")
        )
        .unwrap();
        writeln!(o).unwrap();
        writeln!(o, "| Tag | Variant | Type | |").unwrap();
        writeln!(o, "|---|---|---|---|").unwrap();
        for v in &u.variants {
            writeln!(
                o,
                "| {} | `{}` | {} | {} |",
                v.tag,
                v.name,
                type_name(&v.ty),
                cell(&v.doc)
            )
            .unwrap();
        }
    }
    writeln!(o).unwrap();
    writeln!(o, "### Enums").unwrap();
    for e in &schema.enums {
        writeln!(o).unwrap();
        let repr = if e.repr == Type::U8 { "u8" } else { "u16" };
        writeln!(o, "#### {}", e.name).unwrap();
        writeln!(o).unwrap();
        writeln!(o, "A `{repr}`. {}", e.doc.join(" ")).unwrap();
        writeln!(o).unwrap();
        writeln!(o, "| Value | Name | |").unwrap();
        writeln!(o, "|---|---|---|").unwrap();
        for v in &e.variants {
            writeln!(o, "| {} | `{}` | {} |", v.value, v.name, cell(&v.doc)).unwrap();
        }
    }
    writeln!(o).unwrap();
    write!(o, "{END}").unwrap();
    o
}

/// Replaces the generated section of `readme`, which must contain both markers.
pub fn splice(readme: &str, generated: &str) -> Result<String, String> {
    let start = readme
        .find(BEGIN)
        .ok_or("README.md is missing the BEGIN GENERATED marker")?;
    let end = readme
        .find(END)
        .ok_or("README.md is missing the END GENERATED marker")?
        + END.len();
    Ok(format!("{}{generated}{}", &readme[..start], &readme[end..]))
}
