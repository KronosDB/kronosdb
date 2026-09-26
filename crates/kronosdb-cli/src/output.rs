//! Plain-text rendering shared by the headless commands and the TUI.

use serde_json::Value;

use crate::client::pb;

/// Left-aligned columns padded to the widest cell.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: Vec<&str>| {
        cells
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("{cell:<width$}", width = widths[i]))
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut out = vec![line(headers.to_vec())];
    out.extend(
        rows.iter()
            .map(|row| line(row.iter().map(String::as_str).collect())),
    );
    out.join("\n")
}

pub fn str_of<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

pub fn num_of(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

pub fn bool_of(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

pub fn items(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}

pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn duration(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        3600..86400 => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
        _ => format!("{}d{:02}h", secs / 86400, secs % 86400 / 3600),
    }
}

/// `MM-DD HH:MM:SS` in UTC from epoch milliseconds. Hand-rolled (the civil-
/// from-days algorithm) to avoid a date dependency for one column.
pub fn timestamp(ms: i64) -> String {
    if ms <= 0 {
        return "—".to_string();
    }
    let secs = ms / 1000;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    format!(
        "{month:02}-{day:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Payloads are opaque bytes; show them as text when they are text.
pub fn payload_preview(payload: &[u8], max: usize) -> String {
    match std::str::from_utf8(payload) {
        Ok(text) => {
            let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
            if flat.chars().count() > max {
                format!("{}…", flat.chars().take(max).collect::<String>())
            } else {
                flat
            }
        }
        Err(_) => format!("<{} bytes binary>", payload.len()),
    }
}

pub fn tag_text(tag: &pb::Tag) -> String {
    format!(
        "{}={}",
        String::from_utf8_lossy(&tag.key),
        String::from_utf8_lossy(&tag.value)
    )
}

pub fn event_json(event: &pb::SequencedEvent) -> Value {
    let inner = event.event.clone().unwrap_or_default();
    let payload = match std::str::from_utf8(&inner.payload) {
        Ok(text) => serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string())),
        Err(_) => Value::String(format!("<{} bytes binary>", inner.payload.len())),
    };
    serde_json::json!({
        "sequence": event.sequence,
        "id": inner.identifier,
        "name": inner.name,
        "version": inner.version,
        "timestamp": inner.timestamp,
        "metadata": inner.metadata,
        "payload": payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_pads_to_widest() {
        let out = table(
            &["NAME", "HEAD"],
            &[
                vec!["default".into(), "10".into()],
                vec!["orders-eu".into(), "2".into()],
            ],
        );
        assert_eq!(out, "NAME       HEAD\ndefault    10\norders-eu  2");
    }

    #[test]
    fn humanized() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1536), "1.5 KB");
        assert_eq!(duration(59), "59s");
        assert_eq!(duration(3725), "1h02m");
        // 2026-09-20T10:20:41Z, and a leap day.
        assert_eq!(timestamp(1_789_899_641_000), "09-20 10:20:41");
        assert_eq!(timestamp(1_709_164_800_000), "02-29 00:00:00");
        assert_eq!(timestamp(0), "—");
        assert_eq!(payload_preview(b"{\n \"a\": 1\n}", 40), "{ \"a\": 1 }");
        assert_eq!(payload_preview(&[0xff, 0xfe], 40), "<2 bytes binary>");
        assert_eq!(payload_preview(b"abcdef", 3), "abc…");
    }
}
