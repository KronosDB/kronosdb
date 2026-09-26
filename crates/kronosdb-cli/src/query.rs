//! Event queries, in the one shape KronosDB clients use everywhere: the DCB
//! specification's query items, exactly as kronos-ts writes them
//! (`core/src/event-sourcing/dcb-query.ts`). A query is plain data — one
//! item, or an array of items ORed together; within an item `types` is an
//! ANY-OF and `tags` an ALL-OF. Both are optional: `{}` matches every event.
//!
//! ```text
//! {"tags": {"orderId": "o-1"}}
//! {"tags": {"orderId": "o-1"}, "types": ["OrderPlaced", "OrderPaid"]}
//! [{"tags": {"orderId": "o-1"}}, {"tags": {"customerId": "c-9"}}]
//! ```
//!
//! The same literal is valid as an append condition, because in a DCB model
//! those are the same query. There is deliberately no second spelling.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use crate::client::pb;

/// One query item. Unknown keys are rejected: `{"tag": ...}` silently
/// matching every event would be a nasty way to learn about a typo.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryItem {
    #[serde(default)]
    tags: BTreeMap<String, String>,
    #[serde(default)]
    types: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EventQuery {
    // Order matters: serde will happily read a struct from a JSON array
    // (positional fields), so `[]` would parse as one match-everything item.
    Many(Vec<QueryItem>),
    One(QueryItem),
}

const SHAPE: &str = r#"expected a query item like {"tags": {"orderId": "o-1"}, "types": ["OrderPlaced"]}, or an array of them"#;

/// Parses a query literal into DCB criteria.
pub fn parse(input: &str) -> Result<Vec<pb::Criterion>> {
    let query: EventQuery = serde_json::from_str(input.trim())
        .with_context(|| format!("not an event query — {SHAPE}"))?;
    let items = match query {
        EventQuery::One(item) => vec![item],
        EventQuery::Many(items) if items.is_empty() => {
            bail!("an empty array matches nothing — use {{}} to match every event")
        }
        EventQuery::Many(items) => items,
    };
    // An item with neither field matches everything, and ORing anything with
    // everything is everything: the server spells that as no criteria.
    if items
        .iter()
        .any(|i| i.tags.is_empty() && i.types.is_empty())
    {
        return Ok(vec![]);
    }
    Ok(items
        .into_iter()
        .map(|item| pb::Criterion {
            names: item.types,
            tags: item
                .tags
                .into_iter()
                .map(|(key, value)| pb::Tag {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                })
                .collect(),
        })
        .collect())
}

/// What the TUI's filter prompt accepts: the flag form typed on one line —
/// `orderId=o-1 type=OrderPaid` — because in the index the type is just
/// another tag. Several `type=` are alternatives; tags must all match. A
/// pasted query literal (`{...}` / `[...]`) works too. Empty = no filter.
pub fn parse_filter(input: &str) -> Result<Vec<pb::Criterion>> {
    let trimmed = input.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return parse(trimmed);
    }
    let (mut names, mut tags) = (Vec::new(), Vec::new());
    for pair in trimmed.split_whitespace() {
        match pair.split_once('=') {
            Some(("type", value)) if !value.is_empty() => names.push(value.to_string()),
            Some((key, _)) if !key.is_empty() => tags.push(pair.to_string()),
            _ => bail!("{pair:?} is not key=value (use type=Name for the event type)"),
        }
    }
    from_flags(&names, &tags)
}

/// Builds criteria from the flag form (`--type A --tag k=v`): one criterion.
pub fn from_flags(names: &[String], tags: &[String]) -> Result<Vec<pb::Criterion>> {
    if names.is_empty() && tags.is_empty() {
        return Ok(vec![]);
    }
    let mut criterion = pb::Criterion {
        names: names.to_vec(),
        tags: vec![],
    };
    for tag in tags {
        criterion.tags.push(parse_tag(tag)?);
    }
    Ok(vec![criterion])
}

pub fn parse_tag(text: &str) -> Result<pb::Tag> {
    let Some((key, value)) = text.split_once('=') else {
        bail!("tag {text:?} is not key=value");
    };
    Ok(pb::Tag {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(key: &str, value: &str) -> pb::Tag {
        pb::Tag {
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn everything() {
        assert!(parse("{}").unwrap().is_empty());
        assert!(parse(r#"[{"tags": {"a": "1"}}, {}]"#).unwrap().is_empty());
    }

    #[test]
    fn one_item() {
        assert_eq!(
            parse(r#"{"tags": {"orderId": "o-1", "region": "eu"}, "types": ["OrderPlaced", "OrderPaid"]}"#)
                .unwrap(),
            vec![pb::Criterion {
                names: vec!["OrderPlaced".into(), "OrderPaid".into()],
                tags: vec![tag("orderId", "o-1"), tag("region", "eu")],
            }]
        );
        assert_eq!(
            parse(r#"{"types": ["CustomerRegistered"]}"#).unwrap(),
            vec![pb::Criterion {
                names: vec!["CustomerRegistered".into()],
                tags: vec![],
            }]
        );
    }

    #[test]
    fn array_is_or() {
        let criteria =
            parse(r#"[{"tags": {"orderId": "o-1"}}, {"tags": {"customerId": "c-9"}}]"#).unwrap();
        assert_eq!(criteria.len(), 2);
        assert_eq!(criteria[1].tags, vec![tag("customerId", "c-9")]);
    }

    #[test]
    fn mistakes_are_loud() {
        for input in [
            r#"{"tag": {"orderId": "o-1"}}"#, // typo'd key
            r#"{"tags": {"orderId": 5}}"#,    // tag values are strings
            r#"{"types": "OrderPlaced"}"#,    // types is a list
            "orderId = o-1",                  // not the query shape
            "[]",
        ] {
            assert!(parse(input).is_err(), "{input} should be rejected");
        }
        let err = format!("{:#}", parse("orderId = o-1").unwrap_err());
        assert!(err.contains(r#"{"tags""#), "{err}");
    }

    #[test]
    fn filter_prompt() {
        assert!(parse_filter("  ").unwrap().is_empty());
        let criteria = parse_filter("orderId=o-1 type=OrderPaid type=OrderPlaced").unwrap();
        assert_eq!(criteria.len(), 1);
        assert_eq!(criteria[0].names, vec!["OrderPaid", "OrderPlaced"]);
        assert_eq!(criteria[0].tags, vec![tag("orderId", "o-1")]);
        // A pasted literal is the same query.
        assert_eq!(
            parse_filter(r#"{"tags":{"orderId":"o-1"},"types":["OrderPaid","OrderPlaced"]}"#)
                .unwrap(),
            criteria
        );
        assert!(parse_filter("orderId").is_err());
        assert!(parse_filter("=x").is_err());
    }

    #[test]
    fn flag_form() {
        assert!(from_flags(&[], &[]).unwrap().is_empty());
        let criteria = from_flags(&["A".into()], &["k=v=w".into()]).unwrap();
        assert_eq!(criteria[0].tags, vec![tag("k", "v=w")]);
        assert!(from_flags(&[], &["nope".into()]).is_err());
    }
}
