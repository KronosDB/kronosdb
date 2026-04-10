use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// In-memory registry of event processor state reported by connected clients.
///
/// Clients periodically send `EventProcessorInfo` on the platform stream.
/// This registry stores the latest report per (processor_name, client_id) so
/// the admin console can display processor status, lag, and segment details.
pub struct ProcessorRegistry {
    /// Key: (processor_name, client_id)
    entries: RwLock<HashMap<(String, String), ProcessorEntry>>,
}

/// A single processor report from one client instance.
#[derive(Debug, Clone)]
pub struct ProcessorEntry {
    pub processor_name: String,
    pub mode: String,
    pub client_id: String,
    pub component_name: String,
    pub active_threads: i32,
    pub available_threads: i32,
    pub running: bool,
    pub error: bool,
    pub is_streaming: bool,
    pub token_store_identifier: String,
    pub segments: Vec<SegmentInfo>,
    pub last_reported: Instant,
}

/// Status of a single segment within a streaming event processor.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub segment_id: i32,
    pub caught_up: bool,
    pub replaying: bool,
    pub one_part_of: i32,
    pub token_position: i64,
    pub error_state: String,
}

/// Aggregated view of a processor across all client instances (for admin display).
#[derive(Debug, Clone)]
pub struct ProcessorView {
    pub processor_name: String,
    pub mode: String,
    pub token_store_identifier: String,
    pub is_streaming: bool,
    pub instances: Vec<ProcessorInstanceView>,
}

/// One client instance's contribution to a processor (for admin display).
#[derive(Debug, Clone)]
pub struct ProcessorInstanceView {
    pub client_id: String,
    pub component_name: String,
    pub running: bool,
    pub error: bool,
    pub active_threads: i32,
    pub available_threads: i32,
    pub segments: Vec<SegmentInfo>,
    pub since_last_report: Duration,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Upsert a processor report from a client.
    pub fn report(
        &self,
        client_id: &str,
        component_name: &str,
        info: &crate::proto::kronosdb::platform::EventProcessorInfo,
    ) {
        let segments: Vec<SegmentInfo> = info
            .segment_status
            .iter()
            .map(|s| SegmentInfo {
                segment_id: s.segment_id,
                caught_up: s.caught_up,
                replaying: s.replaying,
                one_part_of: s.one_part_of,
                token_position: s.token_position,
                error_state: s.error_state.clone(),
            })
            .collect();

        let entry = ProcessorEntry {
            processor_name: info.processor_name.clone(),
            mode: info.mode.clone(),
            client_id: client_id.to_string(),
            component_name: component_name.to_string(),
            active_threads: info.active_threads,
            available_threads: info.available_threads,
            running: info.running,
            error: info.error,
            is_streaming: info.is_streaming_processor,
            token_store_identifier: info.token_store_identifier.clone(),
            segments,
            last_reported: Instant::now(),
        };

        let key = (info.processor_name.clone(), client_id.to_string());
        let mut entries = self.entries.write();
        entries.insert(key, entry);
    }

    /// Remove all entries for a disconnected client.
    pub fn remove_client(&self, client_id: &str) {
        let mut entries = self.entries.write();
        entries.retain(|k, _| k.1 != client_id);
    }

    /// Returns all processor entries (flat list).
    pub fn list(&self) -> Vec<ProcessorEntry> {
        let entries = self.entries.read();
        entries.values().cloned().collect()
    }

    /// Returns processors aggregated by (processor_name, token_store_identifier),
    /// with per-instance details grouped together.
    pub fn list_aggregated(&self) -> Vec<ProcessorView> {
        let now = Instant::now();
        let entries = self.entries.read();

        // Group by (processor_name, token_store_identifier).
        let mut groups: HashMap<(String, String), Vec<&ProcessorEntry>> = HashMap::new();
        for entry in entries.values() {
            let key = (
                entry.processor_name.clone(),
                entry.token_store_identifier.clone(),
            );
            groups.entry(key).or_default().push(entry);
        }

        let mut views: Vec<ProcessorView> = groups
            .into_iter()
            .map(|((name, token_store), instances)| {
                let first = &instances[0];
                ProcessorView {
                    processor_name: name,
                    mode: first.mode.clone(),
                    token_store_identifier: token_store,
                    is_streaming: first.is_streaming,
                    instances: instances
                        .into_iter()
                        .map(|e| ProcessorInstanceView {
                            client_id: e.client_id.clone(),
                            component_name: e.component_name.clone(),
                            running: e.running,
                            error: e.error,
                            active_threads: e.active_threads,
                            available_threads: e.available_threads,
                            segments: e.segments.clone(),
                            since_last_report: now.duration_since(e.last_reported),
                        })
                        .collect(),
                }
            })
            .collect();

        views.sort_by(|a, b| a.processor_name.cmp(&b.processor_name));
        views
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kronosdb::platform::{
        EventProcessorInfo, event_processor_info::SegmentStatus,
    };

    fn make_info(name: &str, running: bool, position: i64) -> EventProcessorInfo {
        EventProcessorInfo {
            processor_name: name.to_string(),
            mode: "Tracking".to_string(),
            active_threads: 1,
            running,
            error: false,
            segment_status: vec![SegmentStatus {
                segment_id: 0,
                caught_up: position >= 100,
                replaying: false,
                one_part_of: 1,
                token_position: position,
                error_state: String::new(),
            }],
            available_threads: 0,
            token_store_identifier: "default".to_string(),
            is_streaming_processor: true,
        }
    }

    #[test]
    fn report_and_list() {
        let registry = ProcessorRegistry::new();
        registry.report(
            "client-1",
            "order-service",
            &make_info("OrderProjection", true, 50),
        );
        registry.report(
            "client-2",
            "order-service",
            &make_info("OrderProjection", true, 45),
        );

        let entries = registry.list();
        assert_eq!(entries.len(), 2);

        let views = registry.list_aggregated();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].processor_name, "OrderProjection");
        assert_eq!(views[0].instances.len(), 2);
    }

    #[test]
    fn remove_client() {
        let registry = ProcessorRegistry::new();
        registry.report("client-1", "svc", &make_info("Proc1", true, 10));
        registry.report("client-2", "svc", &make_info("Proc1", true, 20));

        registry.remove_client("client-1");
        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.list()[0].client_id, "client-2");
    }

    #[test]
    fn upsert_overwrites() {
        let registry = ProcessorRegistry::new();
        registry.report("client-1", "svc", &make_info("Proc1", true, 10));
        registry.report("client-1", "svc", &make_info("Proc1", true, 50));

        let entries = registry.list();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].segments[0].token_position, 50);
    }
}
