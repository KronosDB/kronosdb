//! The schema language: a line-oriented description of every type and frame
//! on the wire. See `protocol/kronosdb.schema` for the grammar by example.

use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Width {
    W16,
    W32,
}

impl Width {
    pub fn bits(self) -> u32 {
        match self {
            Width::W16 => 16,
            Width::W32 => 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    U8,
    U16,
    U32,
    U64,
    I64,
    Bool,
    F64,
    Millis,
    Str(Width),
    Bytes(Width),
    List(Width, Box<Type>),
    Opt(Box<Type>),
    /// One stored event record, framed as on disk.
    Record,
    /// u32 count of records, then the records.
    Records,
    /// An enum, struct or union declared in the schema.
    Named(String),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    pub doc: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Struct {
    pub name: String,
    pub doc: Vec<String>,
    /// Frame kind for frames; `None` for plain structs.
    pub kind: Option<u16>,
    pub fields: Vec<Field>,
}

impl Struct {
    pub fn sent_by_server(&self) -> bool {
        self.kind.is_some_and(|k| k & 0x8000 != 0)
    }
}

#[derive(Debug, Clone)]
pub struct Variant {
    pub name: String,
    pub value: u64,
    pub doc: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Enum {
    pub name: String,
    pub doc: Vec<String>,
    /// Wire representation: `Type::U8` or `Type::U16`.
    pub repr: Type,
    pub variants: Vec<Variant>,
}

/// One alternative of a union: a tag byte, then a value of `ty`.
#[derive(Debug, Clone)]
pub struct UnionVariant {
    pub name: String,
    pub tag: u8,
    pub ty: Type,
    pub doc: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Union {
    pub name: String,
    pub doc: Vec<String>,
    pub variants: Vec<UnionVariant>,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub version: u16,
    pub enums: Vec<Enum>,
    pub unions: Vec<Union>,
    /// Structs and frames in declaration order.
    pub structs: Vec<Struct>,
}

/// The struct every `record` body decodes as.
pub const EVENT_RECORD: &str = "EventRecord";

impl Schema {
    pub fn enum_named(&self, name: &str) -> Option<&Enum> {
        self.enums.iter().find(|e| e.name == name)
    }

    pub fn struct_named(&self, name: &str) -> Option<&Struct> {
        self.structs.iter().find(|s| s.name == name)
    }

    pub fn union_named(&self, name: &str) -> Option<&Union> {
        self.unions.iter().find(|u| u.name == name)
    }

    pub fn frames(&self) -> impl Iterator<Item = &Struct> {
        self.structs.iter().filter(|s| s.kind.is_some())
    }

    /// Whether values of `ty` borrow from the frame buffer when decoded.
    pub fn borrows(&self, ty: &Type) -> bool {
        match ty {
            Type::Str(_) | Type::Bytes(_) | Type::Record | Type::Records => true,
            Type::List(_, inner) | Type::Opt(inner) => self.borrows(inner),
            Type::Named(name) => {
                if let Some(s) = self.struct_named(name) {
                    s.fields.iter().any(|f| self.borrows(&f.ty))
                } else if let Some(u) = self.union_named(name) {
                    u.variants.iter().any(|v| self.borrows(&v.ty))
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn struct_borrows(&self, s: &Struct) -> bool {
        s.fields.iter().any(|f| self.borrows(&f.ty))
    }
}

#[derive(Debug)]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "kronosdb.schema:{}: {}", self.line, self.message)
    }
}

fn err<T>(line: usize, message: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError {
        line,
        message: message.into(),
    })
}

pub fn parse_type(src: &str, line: usize) -> Result<Type, ParseError> {
    let src = src.trim();
    if let Some(inner) = generic(src, "list16") {
        return Ok(Type::List(Width::W16, Box::new(parse_type(inner, line)?)));
    }
    if let Some(inner) = generic(src, "list32") {
        return Ok(Type::List(Width::W32, Box::new(parse_type(inner, line)?)));
    }
    if let Some(inner) = generic(src, "opt") {
        let inner = parse_type(inner, line)?;
        if matches!(inner, Type::Opt(_)) {
            return err(line, "opt<opt<..>> is not allowed");
        }
        return Ok(Type::Opt(Box::new(inner)));
    }
    Ok(match src {
        "u8" => Type::U8,
        "u16" => Type::U16,
        "u32" => Type::U32,
        "u64" => Type::U64,
        "i64" => Type::I64,
        "bool" => Type::Bool,
        "f64" => Type::F64,
        "millis" => Type::Millis,
        "str16" => Type::Str(Width::W16),
        "str32" => Type::Str(Width::W32),
        "bytes16" => Type::Bytes(Width::W16),
        "bytes32" => Type::Bytes(Width::W32),
        "record" => Type::Record,
        "records" => Type::Records,
        name if is_type_name(name) => Type::Named(name.to_string()),
        other => return err(line, format!("unknown type `{other}`")),
    })
}

fn generic<'a>(src: &'a str, head: &str) -> Option<&'a str> {
    src.strip_prefix(head)?
        .trim()
        .strip_prefix('<')?
        .strip_suffix('>')
}

fn is_type_name(s: &str) -> bool {
    s.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && s.chars().all(|c| c.is_ascii_alphanumeric())
}

fn is_field_name(s: &str) -> bool {
    s.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn parse_int(s: &str, line: usize) -> Result<u64, ParseError> {
    let s = s.trim();
    let parsed = match s.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => s.parse(),
    };
    parsed.or_else(|_| err(line, format!("not a number: `{s}`")))
}

enum Open {
    None,
    Enum(Enum),
    Struct(Struct),
    Union(Union),
}

pub fn parse(src: &str) -> Result<Schema, ParseError> {
    let mut schema = Schema {
        name: String::new(),
        version: 0,
        enums: Vec::new(),
        unions: Vec::new(),
        structs: Vec::new(),
    };
    let mut doc: Vec<String> = Vec::new();
    let mut open = Open::None;

    for (index, raw) in src.lines().enumerate() {
        let line = index + 1;
        let text = raw.trim();
        if let Some(d) = text.strip_prefix("///") {
            doc.push(d.strip_prefix(' ').unwrap_or(d).to_string());
            continue;
        }
        if text.is_empty() || text.starts_with("//") {
            doc.clear();
            continue;
        }
        let taken = std::mem::take(&mut doc);

        match &mut open {
            Open::None => {
                if let Some(rest) = text.strip_prefix("protocol ") {
                    let mut parts = rest.split_whitespace();
                    schema.name = parts.next().unwrap_or_default().to_string();
                    schema.version = parse_int(parts.next().unwrap_or_default(), line)? as u16;
                } else if let Some(rest) = text.strip_prefix("enum ") {
                    let Some(head) = rest.strip_suffix('{') else {
                        return err(line, "expected `enum Name: u8|u16 {`");
                    };
                    let Some((name, repr)) = head.split_once(':') else {
                        return err(line, "an enum needs a representation: `enum Name: u16 {`");
                    };
                    let repr = match repr.trim() {
                        "u8" => Type::U8,
                        "u16" => Type::U16,
                        other => {
                            return err(
                                line,
                                format!("enum repr must be u8 or u16, not `{other}`"),
                            );
                        }
                    };
                    open = Open::Enum(Enum {
                        name: name.trim().to_string(),
                        doc: taken,
                        repr,
                        variants: Vec::new(),
                    });
                } else if let Some(rest) = text.strip_prefix("union ") {
                    let Some(name) = rest.strip_suffix('{') else {
                        return err(line, "expected `union Name {`");
                    };
                    open = Open::Union(Union {
                        name: name.trim().to_string(),
                        doc: taken,
                        variants: Vec::new(),
                    });
                } else if let Some(rest) = text.strip_prefix("struct ") {
                    let Some(name) = rest.strip_suffix('{') else {
                        return err(line, "expected `struct Name {`");
                    };
                    open = Open::Struct(Struct {
                        name: name.trim().to_string(),
                        doc: taken,
                        kind: None,
                        fields: Vec::new(),
                    });
                } else if let Some(rest) = text.strip_prefix("frame ") {
                    let Some(head) = rest.strip_suffix('{') else {
                        return err(line, "expected `frame Name = 0x0000 {`");
                    };
                    let Some((name, kind)) = head.split_once('=') else {
                        return err(line, "a frame needs a kind: `frame Name = 0x0100 {`");
                    };
                    let kind = parse_int(kind, line)?;
                    if kind > u16::MAX as u64 {
                        return err(line, "frame kinds are u16");
                    }
                    open = Open::Struct(Struct {
                        name: name.trim().to_string(),
                        doc: taken,
                        kind: Some(kind as u16),
                        fields: Vec::new(),
                    });
                } else {
                    return err(line, format!("unexpected `{text}`"));
                }
            }
            Open::Enum(e) => {
                if text == "}" {
                    let Open::Enum(e) = std::mem::replace(&mut open, Open::None) else {
                        unreachable!()
                    };
                    schema.enums.push(e);
                    continue;
                }
                let Some((name, value)) = text.split_once('=') else {
                    return err(line, "expected `variant = value`");
                };
                e.variants.push(Variant {
                    name: name.trim().to_string(),
                    value: parse_int(value, line)?,
                    doc: taken,
                });
            }
            Open::Union(u) => {
                if text == "}" {
                    let Open::Union(u) = std::mem::replace(&mut open, Open::None) else {
                        unreachable!()
                    };
                    schema.unions.push(u);
                    continue;
                }
                let parsed = text.split_once('=').and_then(|(name, rest)| {
                    let (tag, ty) = rest.split_once(':')?;
                    Some((name.trim(), tag.trim(), ty))
                });
                let Some((name, tag, ty)) = parsed else {
                    return err(line, "expected `variant = tag: type`");
                };
                let tag = parse_int(tag, line)?;
                if tag == 0 || tag > u8::MAX as u64 {
                    return err(line, "union tags are 1..=255");
                }
                u.variants.push(UnionVariant {
                    name: name.to_string(),
                    tag: tag as u8,
                    ty: parse_type(ty, line)?,
                    doc: taken,
                });
            }
            Open::Struct(s) => {
                if text == "}" {
                    let Open::Struct(s) = std::mem::replace(&mut open, Open::None) else {
                        unreachable!()
                    };
                    schema.structs.push(s);
                    continue;
                }
                let Some((name, ty)) = text.split_once(':') else {
                    return err(line, "expected `field: type`");
                };
                s.fields.push(Field {
                    name: name.trim().to_string(),
                    ty: parse_type(ty, line)?,
                    doc: taken,
                });
            }
        }
    }
    if !matches!(open, Open::None) {
        return err(src.lines().count(), "unterminated block");
    }
    validate(&schema)?;
    Ok(schema)
}

fn validate(schema: &Schema) -> Result<(), ParseError> {
    let fail = |message: String| err::<()>(0, message);
    if schema.name.is_empty() || schema.version == 0 {
        fail("missing `protocol <name> <version>`".into())?;
    }
    let mut names = HashSet::new();
    for name in schema
        .enums
        .iter()
        .map(|e| &e.name)
        .chain(schema.unions.iter().map(|u| &u.name))
        .chain(schema.structs.iter().map(|s| &s.name))
    {
        if !is_type_name(name) {
            fail(format!("`{name}` must be UpperCamelCase"))?;
        }
        if !names.insert(name.clone()) {
            fail(format!("`{name}` is declared twice"))?;
        }
    }
    for e in &schema.enums {
        let max = if e.repr == Type::U8 {
            u8::MAX as u64
        } else {
            u16::MAX as u64
        };
        let mut values = HashSet::new();
        for v in &e.variants {
            if !is_field_name(&v.name) {
                fail(format!("{}.{} must be snake_case", e.name, v.name))?;
            }
            if v.value > max || !values.insert(v.value) {
                fail(format!(
                    "{}.{} has a duplicate or out-of-range value",
                    e.name, v.name
                ))?;
            }
        }
    }
    for u in &schema.unions {
        if u.variants.is_empty() {
            fail(format!("union {} has no variants", u.name))?;
        }
        let mut tags = HashSet::new();
        for v in &u.variants {
            if !is_field_name(&v.name) {
                fail(format!("{}.{} must be snake_case", u.name, v.name))?;
            }
            if !tags.insert(v.tag) {
                fail(format!("{}.{} reuses tag {}", u.name, v.name, v.tag))?;
            }
            check_type(schema, &u.name, &v.ty, &mut vec![u.name.clone()])?;
        }
    }
    let mut kinds: HashMap<u16, &str> = HashMap::new();
    for s in &schema.structs {
        if let Some(kind) = s.kind {
            if kind == 0 || kind == 0x8000 {
                fail(format!("{}: kinds 0x0000 and 0x8000 are reserved", s.name))?;
            }
            if let Some(other) = kinds.insert(kind, &s.name) {
                fail(format!("{} and {other} share kind {kind:#06x}", s.name))?;
            }
        }
        let mut fields = HashSet::new();
        for f in &s.fields {
            if !is_field_name(&f.name) {
                fail(format!("{}.{} must be snake_case", s.name, f.name))?;
            }
            if !fields.insert(&f.name) {
                fail(format!("{}.{} is declared twice", s.name, f.name))?;
            }
            check_type(schema, &s.name, &f.ty, &mut vec![s.name.clone()])?;
        }
    }
    match schema.struct_named(EVENT_RECORD) {
        Some(s) if s.kind.is_none() => {}
        _ => fail(format!(
            "`struct {EVENT_RECORD}` must exist: `record` bodies decode as it"
        ))?,
    }
    Ok(())
}

fn check_type(
    schema: &Schema,
    owner: &str,
    ty: &Type,
    path: &mut Vec<String>,
) -> Result<(), ParseError> {
    match ty {
        Type::List(_, inner) | Type::Opt(inner) => check_type(schema, owner, inner, path),
        Type::Named(name) => {
            if schema.enum_named(name).is_some() {
                return Ok(());
            }
            if let Some(u) = schema.union_named(name) {
                if path.contains(name) {
                    return err(0, format!("{owner}: `{name}` is recursive"));
                }
                path.push(name.clone());
                for v in &u.variants {
                    check_type(schema, owner, &v.ty, path)?;
                }
                path.pop();
                return Ok(());
            }
            let Some(target) = schema.struct_named(name) else {
                return err(0, format!("{owner}: unknown type `{name}`"));
            };
            if target.kind.is_some() {
                return err(0, format!("{owner}: frames cannot be nested (`{name}`)"));
            }
            if path.contains(name) {
                return err(0, format!("{owner}: `{name}` is recursive"));
            }
            path.push(name.clone());
            for f in &target.fields {
                check_type(schema, owner, &f.ty, path)?;
            }
            path.pop();
            Ok(())
        }
        _ => Ok(()),
    }
}
