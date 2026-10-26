use std::collections::HashMap;

use parking_lot::RwLock;

use crate::hub::ProcessorReport;

/// In-memory registry of event processor state reported by connected clients.
///
/// Clients periodically report their processors on the platform stream.
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
    pub running: bool,
    pub error: bool,
    pub is_streaming: bool,
    pub token_store_identifier: String,
    pub segments: Vec<SegmentInfo>,
}

/// Status of a single segment within a streaming event processor.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub segment_id: u32,
    pub caught_up: bool,
    pub replaying: bool,
    pub one_part_of: u32,
    pub token_position: i64,
    pub error_state: String,
}

/// Aggregated view of a processor across all client instances (for admin display).
#[derive(Debug, Clone)]
pub struct ProcessorView {
    pub processor_name: String,
    pub mode: String,
    pub is_streaming: bool,
    pub instances: Vec<ProcessorInstanceView>,
}

/// One client instance's contribution to a processor (for admin display).
#[derive(Debug, Clone)]
pub struct ProcessorInstanceView {
    pub running: bool,
    pub error: bool,
    pub segments: Vec<SegmentInfo>,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Upsert a processor report from a client.
    pub fn report(&self, client_id: &str, report: ProcessorReport) {
        let segments: Vec<SegmentInfo> = report
            .segments
            .into_iter()
            .map(|s| SegmentInfo {
                segment_id: s.segment_id,
                caught_up: s.caught_up,
                replaying: s.replaying,
                one_part_of: s.one_part_of,
                token_position: s.token_position,
                error_state: s.error_state,
            })
            .collect();

        let key = (report.processor_name.clone(), client_id.to_string());
        let entry = ProcessorEntry {
            processor_name: report.processor_name,
            mode: report.mode,
            client_id: client_id.to_string(),
            running: report.running,
            error: report.error,
            is_streaming: report.streaming,
            token_store_identifier: report.token_store_identifier,
            segments,
        };
        self.entries.write().insert(key, entry);
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
            .map(|((name, _token_store), instances)| {
                let first = &instances[0];
                ProcessorView {
                    processor_name: name,
                    mode: first.mode.clone(),
                    is_streaming: first.is_streaming,
                    instances: instances
                        .into_iter()
                        .map(|e| ProcessorInstanceView {
                            running: e.running,
                            error: e.error,
                            segments: e.segments.clone(),
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
    use crate::hub::SegmentReport;

    fn make_info(name: &str, running: bool, position: i64) -> ProcessorReport {
        ProcessorReport {
            processor_name: name.to_string(),
            mode: "Tracking".to_string(),
            running,
            error: false,
            segments: vec![SegmentReport {
                segment_id: 0,
                caught_up: position >= 100,
                replaying: false,
                one_part_of: 1,
                token_position: position,
                error_state: String::new(),
            }],
            token_store_identifier: "default".to_string(),
            streaming: true,
        }
    }

    #[test]
    fn report_and_list() {
        let registry = ProcessorRegistry::new();
        registry.report("client-1", make_info("OrderProjection", true, 50));
        registry.report("client-2", make_info("OrderProjection", true, 45));

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
        registry.report("client-1", make_info("Proc1", true, 10));
        registry.report("client-2", make_info("Proc1", true, 20));

        registry.remove_client("client-1");
        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.list()[0].client_id, "client-2");
    }

    #[test]
    fn upsert_overwrites() {
        let registry = ProcessorRegistry::new();
        registry.report("client-1", make_info("Proc1", true, 10));
        registry.report("client-1", make_info("Proc1", true, 50));

        let entries = registry.list();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].segments[0].token_position, 50);
    }
}
