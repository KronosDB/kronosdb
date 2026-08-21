//! Command bus dispatch micro-benchmarks.
//!
//! Measures the server-side cost of one dispatch decision: capacity check,
//! handler selection (weighted round-robin or consistent-hash ring), permit
//! acquisition, and in-flight registration. Each iteration cancels the
//! command afterwards so the in-flight map stays bounded — the measured
//! unit is a full register/deregister cycle, which is what every real
//! dispatch pays. gRPC transport and handler execution are out of scope.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kronosdb_messaging::command::{Command, CommandBus};
use kronosdb_messaging::types::{ClientId, ComponentName, Payload, RoutingKey};
use std::collections::HashMap;
use std::hint::black_box;

fn make_bus(handlers: usize) -> CommandBus {
    let bus = CommandBus::new();
    for i in 0..handlers {
        let client = ClientId(format!("handler-{i}"));
        bus.subscribe(
            "BenchCommand".into(),
            client.clone(),
            ComponentName("bench".into()),
            100,
        );
        bus.grant_permits(&client, i64::MAX / 4);
    }
    bus
}

fn make_command(seq: u64, routing_key: Option<RoutingKey>) -> Command {
    Command {
        message_id: format!("msg-{seq}"),
        name: "BenchCommand".into(),
        timestamp: 0,
        payload: Payload {
            payload_type: "BenchCommand".into(),
            revision: "1".into(),
            data: vec![0u8; 128],
        },
        metadata: HashMap::new(),
        processing_instructions: vec![],
        routing_key,
        client_id: ClientId("bench-dispatcher".into()),
        component_name: ComponentName("bench".into()),
    }
}

fn command_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_dispatch");
    group.throughput(Throughput::Elements(1));

    for &handlers in &[1usize, 4, 16] {
        group.bench_with_input(
            BenchmarkId::new("round_robin", handlers),
            &handlers,
            |b, &n| {
                let bus = make_bus(n);
                let mut seq = 0u64;
                b.iter(|| {
                    seq += 1;
                    let cmd = make_command(seq, None);
                    let id = cmd.message_id.clone();
                    let (pending, _rx) = bus.dispatch(black_box(cmd)).unwrap();
                    black_box(&pending.target_handler);
                    bus.cancel_in_flight(&id);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("routing_key_ring", handlers),
            &handlers,
            |b, &n| {
                let bus = make_bus(n);
                let mut seq = 0u64;
                b.iter(|| {
                    seq += 1;
                    // 1024 distinct keys — realistic aggregate spread.
                    let key = RoutingKey(format!("agg-{}", seq % 1024));
                    let cmd = make_command(seq, Some(key));
                    let id = cmd.message_id.clone();
                    let (pending, _rx) = bus.dispatch(black_box(cmd)).unwrap();
                    black_box(&pending.target_handler);
                    bus.cancel_in_flight(&id);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, command_dispatch);
criterion_main!(benches);
