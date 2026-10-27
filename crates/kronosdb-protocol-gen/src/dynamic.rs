//! The reference codec: encodes and decodes any frame straight from the
//! schema, with JSON as the value model. It produces the golden fixture
//! bytes, so every generated codec is checked against an implementation that
//! shares no code with it.
//!
//! JSON mapping (also what the generated `to_json` helpers produce):
//! u8/u16/u32/millis → number, u64/i64 → decimal string, bool → bool,
//! str → string, bytes → lowercase hex string (input also accepts
//! `{"utf8": "..."}`), list → array, opt → null or the value, enum → variant
//! name (number when unknown), struct → object, union → an object with one
//! key naming the variant, f64 → number, record → `{"flags", "event"}`.

use serde_json::{Map, Value, json};

use crate::schema::{EVENT_RECORD, Schema, Struct, Type, Width};

pub const HEADER_LEN: usize = 11;

pub fn encode_frame(schema: &Schema, fixture: &Value) -> Result<Vec<u8>, String> {
    let name = fixture["frame"].as_str().ok_or("fixture needs \"frame\"")?;
    let frame = schema
        .frames()
        .find(|f| f.name == name)
        .ok_or_else(|| format!("unknown frame `{name}`"))?;
    let stream = fixture["stream"]
        .as_u64()
        .ok_or("fixture needs \"stream\"")?;
    let flags = fixture["flags"].as_u64().ok_or("fixture needs \"flags\"")?;
    let mut body = Vec::new();
    encode_struct(schema, frame, &fixture["body"], &mut body)?;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&((HEADER_LEN - 4 + body.len()) as u32).to_le_bytes());
    out.extend_from_slice(&frame.kind.unwrap().to_le_bytes());
    out.push(u8::try_from(flags).map_err(|_| "flags is a u8")?);
    out.extend_from_slice(
        &u32::try_from(stream)
            .map_err(|_| "stream is a u32")?
            .to_le_bytes(),
    );
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_frame(schema: &Schema, bytes: &[u8]) -> Result<Value, String> {
    let mut c = Cursor { b: bytes, at: 0 };
    let len = c.u32()? as usize;
    if len + 4 != bytes.len() {
        return Err(format!(
            "frame length {len} does not match {} bytes",
            bytes.len()
        ));
    }
    let kind = c.u16()?;
    let flags = c.u8()?;
    let stream = c.u32()?;
    let frame = schema
        .frames()
        .find(|f| f.kind == Some(kind))
        .ok_or_else(|| format!("unknown frame kind {kind:#06x}"))?;
    let body = decode_struct(schema, frame, &mut c)?;
    if c.at != bytes.len() {
        return Err(format!("{} trailing bytes", bytes.len() - c.at));
    }
    Ok(json!({ "frame": frame.name, "stream": stream, "flags": flags, "body": body }))
}

fn encode_struct(schema: &Schema, s: &Struct, v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    let obj = v
        .as_object()
        .ok_or_else(|| format!("{} must be an object", s.name))?;
    for key in obj.keys() {
        if !s.fields.iter().any(|f| &f.name == key) {
            return Err(format!("{} has no field `{key}`", s.name));
        }
    }
    for f in &s.fields {
        let value = obj.get(&f.name).unwrap_or(&Value::Null);
        encode_value(schema, &f.ty, value, out)
            .map_err(|e| format!("{}.{}: {e}", s.name, f.name))?;
    }
    Ok(())
}

fn uint(v: &Value, max: u64) -> Result<u64, String> {
    let n = match v {
        Value::Number(n) => n.as_u64().ok_or("expected an unsigned integer")?,
        Value::String(s) => s
            .parse()
            .map_err(|_| format!("`{s}` is not an unsigned integer"))?,
        _ => return Err(format!("expected an unsigned integer, got {v}")),
    };
    if n > max {
        return Err(format!("{n} does not fit"));
    }
    Ok(n)
}

fn int(v: &Value) -> Result<i64, String> {
    match v {
        Value::Number(n) => n.as_i64().ok_or_else(|| "expected an integer".to_string()),
        Value::String(s) => s.parse().map_err(|_| format!("`{s}` is not an integer")),
        _ => Err(format!("expected an integer, got {v}")),
    }
}

fn bytes_of(v: &Value) -> Result<Vec<u8>, String> {
    match v {
        Value::String(hex) => {
            if hex.len() % 2 != 0 {
                return Err("odd-length hex".into());
            }
            (0..hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| format!("bad hex `{hex}`"))
                })
                .collect()
        }
        Value::Object(o) if o.len() == 1 && o.contains_key("utf8") => Ok(o["utf8"]
            .as_str()
            .ok_or("utf8 must be a string")?
            .as_bytes()
            .to_vec()),
        _ => Err(format!("expected hex bytes, got {v}")),
    }
}

fn put_len(width: Width, len: usize, out: &mut Vec<u8>) -> Result<(), String> {
    match width {
        Width::W16 => out.extend_from_slice(
            &u16::try_from(len)
                .map_err(|_| "longer than u16")?
                .to_le_bytes(),
        ),
        Width::W32 => out.extend_from_slice(
            &u32::try_from(len)
                .map_err(|_| "longer than u32")?
                .to_le_bytes(),
        ),
    }
    Ok(())
}

fn encode_value(schema: &Schema, ty: &Type, v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    match ty {
        Type::U8 => out.push(uint(v, u8::MAX as u64)? as u8),
        Type::U16 => out.extend_from_slice(&(uint(v, u16::MAX as u64)? as u16).to_le_bytes()),
        Type::U32 => out.extend_from_slice(&(uint(v, u32::MAX as u64)? as u32).to_le_bytes()),
        Type::U64 => out.extend_from_slice(&uint(v, u64::MAX)?.to_le_bytes()),
        Type::I64 | Type::Millis => out.extend_from_slice(&int(v)?.to_le_bytes()),
        Type::Bool => out.push(v.as_bool().ok_or("expected a bool")? as u8),
        Type::F64 => out.extend_from_slice(&v.as_f64().ok_or("expected a number")?.to_le_bytes()),
        Type::Str(w) => {
            let s = v.as_str().ok_or("expected a string")?;
            put_len(*w, s.len(), out)?;
            out.extend_from_slice(s.as_bytes());
        }
        Type::Bytes(w) => {
            let b = bytes_of(v)?;
            put_len(*w, b.len(), out)?;
            out.extend_from_slice(&b);
        }
        Type::List(w, inner) => {
            let items = v.as_array().ok_or("expected an array")?;
            put_len(*w, items.len(), out)?;
            for item in items {
                encode_value(schema, inner, item, out)?;
            }
        }
        Type::Opt(inner) => {
            if v.is_null() {
                out.push(0);
            } else {
                out.push(1);
                encode_value(schema, inner, v, out)?;
            }
        }
        Type::Record => encode_record(schema, v, out)?,
        Type::Records => {
            let items = v.as_array().ok_or("expected an array of records")?;
            put_len(Width::W32, items.len(), out)?;
            for item in items {
                encode_record(schema, item, out)?;
            }
        }
        Type::Named(name) => {
            if let Some(e) = schema.enum_named(name) {
                let value = match v {
                    Value::String(s) => {
                        e.variants
                            .iter()
                            .find(|var| &var.name == s)
                            .ok_or_else(|| format!("{name} has no variant `{s}`"))?
                            .value
                    }
                    other => uint(other, u16::MAX as u64)?,
                };
                encode_value(schema, &e.repr, &json!(value), out)?;
            } else if let Some(u) = schema.union_named(name) {
                let obj = v
                    .as_object()
                    .filter(|o| o.len() == 1)
                    .ok_or_else(|| format!("{name} must be an object with one variant key"))?;
                let (key, value) = obj.iter().next().unwrap();
                let variant = u
                    .variants
                    .iter()
                    .find(|var| &var.name == key)
                    .ok_or_else(|| format!("{name} has no variant `{key}`"))?;
                out.push(variant.tag);
                encode_value(schema, &variant.ty, value, out)?;
            } else {
                encode_struct(schema, schema.struct_named(name).unwrap(), v, out)?;
            }
        }
    }
    Ok(())
}

fn encode_record(schema: &Schema, v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    let flags = uint(&v["flags"], u8::MAX as u64)? as u8;
    let mut body = Vec::new();
    encode_struct(
        schema,
        schema.struct_named(EVENT_RECORD).unwrap(),
        &v["event"],
        &mut body,
    )?;
    out.extend_from_slice(&((1 + body.len()) as u32).to_le_bytes());
    out.push(flags);
    out.extend_from_slice(&body);
    Ok(())
}

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let s = self
            .b
            .get(self.at..self.at + n)
            .ok_or_else(|| format!("truncated: need {n} bytes at offset {}", self.at))?;
        self.at += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn len(&mut self, w: Width) -> Result<usize, String> {
        Ok(match w {
            Width::W16 => self.u16()? as usize,
            Width::W32 => self.u32()? as usize,
        })
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn decode_struct(schema: &Schema, s: &Struct, c: &mut Cursor) -> Result<Value, String> {
    let mut obj = Map::new();
    for f in &s.fields {
        let v =
            decode_value(schema, &f.ty, c).map_err(|e| format!("{}.{}: {e}", s.name, f.name))?;
        obj.insert(f.name.clone(), v);
    }
    Ok(Value::Object(obj))
}

fn decode_value(schema: &Schema, ty: &Type, c: &mut Cursor) -> Result<Value, String> {
    Ok(match ty {
        Type::U8 => json!(c.u8()?),
        Type::U16 => json!(c.u16()?),
        Type::U32 => json!(c.u32()?),
        Type::U64 => json!(c.u64()?.to_string()),
        Type::I64 => json!((c.u64()? as i64).to_string()),
        Type::Millis => json!(c.u64()? as i64),
        Type::F64 => json!(f64::from_le_bytes(c.take(8)?.try_into().unwrap())),
        Type::Bool => match c.u8()? {
            0 => json!(false),
            1 => json!(true),
            b => return Err(format!("bool byte {b}")),
        },
        Type::Str(w) => {
            let n = c.len(*w)?;
            let s = std::str::from_utf8(c.take(n)?).map_err(|_| "invalid UTF-8")?;
            json!(s)
        }
        Type::Bytes(w) => {
            let n = c.len(*w)?;
            json!(hex(c.take(n)?))
        }
        Type::List(w, inner) => {
            let n = c.len(*w)?;
            let mut items = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                items.push(decode_value(schema, inner, c)?);
            }
            Value::Array(items)
        }
        Type::Opt(inner) => match c.u8()? {
            0 => Value::Null,
            1 => decode_value(schema, inner, c)?,
            b => return Err(format!("option byte {b}")),
        },
        Type::Record => decode_record(schema, c)?,
        Type::Records => {
            let n = c.u32()? as usize;
            let mut items = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                items.push(decode_record(schema, c)?);
            }
            Value::Array(items)
        }
        Type::Named(name) => {
            if let Some(e) = schema.enum_named(name) {
                let raw = match e.repr {
                    Type::U8 => c.u8()? as u64,
                    _ => c.u16()? as u64,
                };
                match e.variants.iter().find(|v| v.value == raw) {
                    Some(v) => json!(v.name),
                    None => json!(raw),
                }
            } else if let Some(u) = schema.union_named(name) {
                let tag = c.u8()?;
                let variant = u
                    .variants
                    .iter()
                    .find(|v| v.tag == tag)
                    .ok_or_else(|| format!("{name} has no variant with tag {tag}"))?;
                let mut obj = Map::new();
                obj.insert(variant.name.clone(), decode_value(schema, &variant.ty, c)?);
                Value::Object(obj)
            } else {
                decode_struct(schema, schema.struct_named(name).unwrap(), c)?
            }
        }
    })
}

fn decode_record(schema: &Schema, c: &mut Cursor) -> Result<Value, String> {
    let len = c.u32()? as usize;
    if len == 0 {
        return Err("zero-length record".into());
    }
    let flags = c.u8()?;
    let body = c.take(len - 1)?;
    let mut inner = Cursor { b: body, at: 0 };
    let event = decode_struct(
        schema,
        schema.struct_named(EVENT_RECORD).unwrap(),
        &mut inner,
    )?;
    if inner.at != body.len() {
        return Err("record body has trailing bytes".into());
    }
    Ok(json!({ "flags": flags, "event": event }))
}
