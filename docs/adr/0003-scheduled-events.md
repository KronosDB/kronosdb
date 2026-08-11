# ADR-0003: Scheduled events (schedules as events, scheduler as projection)

Date: 2026-08-02
Status: Proposed
Deciders: Theo

## Context

KronosDB has no way to append an event *at a future time*. Clients that
need deadlines (payment timeouts, reminder events, saga deadlines) must
run their own poller that watches the clock and appends when due — a
liveness and correctness burden pushed onto every client, with no story
for client crashes.

Axon Server SE solves this with an `EventScheduler` gRPC service backed
by a task table in its embedded control database: schedule = insert a
row, a 5-minute window poller arms in-memory timers, firing is a plain
unconditional append. Two structural weaknesses:

1. **A second durability domain.** The schedule row lives in a
   single-node H2 file, fsynced and replicated (not at all, in SE)
   independently of the event log it feeds. Losing that file loses every
   pending schedule while the log survives.
2. **No idempotency at fire time.** The fire is an unguarded append.
   With failover, the window between "old leader fired" and "completion
   recorded" is a double-fire.

Two properties of the ADR-0001 architecture dissolve both problems:

- **The log is already the one quorum-durable, replicated store.** A
  schedule persisted *as an event* inherits the watermark ack, byte-exact
  replication, and crash recovery — no second store to build or trust.
- **Conditional append is the core primitive.** Exactly-once firing and
  cancel/fire races are consistency conditions, not coordination
  protocols.

Putting schedules in Raft control-plane entries was considered and
rejected: the scheduled payload is event data, and ADR-0001 forbids
event data in the Raft log.

## Decision

Adopt **scheduled events as first-class log events**, with the scheduler
as a projection over them. A schedule's lifecycle is a set of system
events sharing a per-token tag (`$schedule:{token}`) plus a constant
marker tag so the projection can follow all of them with one local
subscription:

```
$schedule.created      # target event (type/tags/payload), due_ts,
                       # optional fire-time condition
$schedule.rescheduled  # new due_ts, optional replacement payload; same token
$schedule.cancelled    # resolves the schedule without firing
$schedule.superseded   # fire-time condition failed; resolved without firing
<fired user event>     # the target event, + $schedule:{token} tag
```

**Liveness guard.** A schedule is *live* iff no event tagged
`$schedule:{token}` with a resolving type exists. Every state change
appends under the same condition:

```
fire:    append(target event + tag $schedule:{token})
         unless exists(tag=$schedule:{token},
                       type in {<target type>, $schedule.cancelled, $schedule.superseded})
cancel:  append($schedule.cancelled)          — same condition
```

Whichever append commits first wins; the loser fails its condition and
reports it (`CancelSchedule` returns "already fired" rather than a
silent no-op). Double-fire after failover is impossible by construction:
the new leader's fire fails the condition if the old leader's committed.

**Scheduler runtime.** Every node runs the projection (local subscribe on
the marker tag; reads are local per ADR-0001) and keeps an in-memory
due-heap of `(due_ts, token, log_position)` — payloads stay in the log
and are read back by position at fire time, so pending entries cost tens
of bytes and no Axon-style fetch window is needed. Only the fenced
data-plane leader arms timers and fires; on failover the new leader's
heap is already warm — it starts firing, nothing to rebuild. Overdue
schedules (downtime, failover) fire immediately in due-time order; the
fired event's timestamp is the actual append time. Transient fire
failures (no quorum) retry with backoff — safe because the append is
condition-guarded.

**Idempotent creation.** The client may supply the token; `ScheduleAppend`
appends `$schedule.created` under the condition that no created-event
with that token exists. Retrying a timed-out schedule call is then safe.
Server generates a token when absent. Acks follow the configured
ack-mode, exactly like any append.

**Fire-time condition (DCB extension).** `$schedule.created` may carry a
consistency condition evaluated when due — "append `PaymentTimedOut`
unless an event tagged `paymentId=X` typed `PaymentReceived` exists." If
it fails, the scheduler appends `$schedule.superseded` instead: the
deadline pattern without the client-side cancellation race.

**System-event namespace.** Types and tag keys starting with `$` are
reserved for the server: client appends carrying them are rejected, and
system events are **never** returned by the client read path —
`Source`, `Subscribe`, and `Tags` cannot see them, with no opt-in flag.
System tags attached to user events (the fired event's
`$schedule:{token}`) are likewise unmatchable and stripped from
results. Only the internal read path sees them; clients reach that
state through purpose-built APIs (`ListSchedules`) and the admin
console. See `docs/system-events.md` for the governing rules — this ADR
is the first user of that convention.

**Surface.** New `scheduler.proto`, own API (Axon compat stays in the
connector, which maps Axon Framework's `EventScheduler` onto it):

```proto
service SchedulerService {
  rpc ScheduleAppend (ScheduleAppendRequest) returns (ScheduleAppendResponse); // → token
  rpc Reschedule     (RescheduleRequest)     returns (ScheduleAppendResponse); // same token
  rpc CancelSchedule (CancelScheduleRequest) returns (CancelScheduleResponse);
  rpc ListSchedules  (ListSchedulesRequest)  returns (ListSchedulesResponse);  // from projection
}
```

Per-context like everything else (`kronosdb-context` header). The
projection and firing engine live in `kronosdb-eventstore`; the gRPC
service and an admin console page (pending schedules, resolution
history) live in `kronosdb-server`.

## Consequences

- Scheduling inherits every existing guarantee — watermark durability,
  replication, failover, backup (ADR-0002 archives schedule events with
  everything else) — and the audit trail (scheduled → rescheduled →
  fired/cancelled, when, by whom) is sourceable like any history.
- A worst case of two payload copies in the log per schedule (created +
  fired). Accepted: segments are cheap, tiering makes them cheaper, and
  the store never deletes anyway.
- The `$` namespace is new internal surface with implications beyond
  scheduling (future system events ride the same convention). Because
  they are invisible rather than opt-in visible, they stay private
  implementation detail with no stability contract — the API is the
  contract, not the log.
- System events consume log positions, so head advances faster than
  user events and client reads see position gaps. Already normal: any
  filtered `Source` returns non-contiguous positions, so clients must
  handle gaps regardless.
- Fires consume normal append throughput and positions; a burst of
  simultaneously-due schedules is smoothed by the group-commit path like
  any client burst.
- Clock semantics are leader-local: "due" means the fenced leader's
  clock reached `due_ts`. Cross-failover skew can fire an event early or
  late by the inter-node clock delta; ordering among fires is due-time
  order as observed by whichever leader fires them.

## Non-goals

- **No recurring/cron schedules.** One schedule, one fire. Recurrence is
  a client rescheduling on receipt of the fired event (or a future ADR).
- **No scheduled commands/queries.** Only appends. The messaging layer
  is not in scope.
- **No generic task framework.** Axon generalized to "scheduled tasks
  with pluggable executors"; per the no-plugin-system policy, this is a
  scheduler for exactly one action: conditional append.
- **No cross-context schedules.** A schedule lives, fires, and resolves
  in one context.
