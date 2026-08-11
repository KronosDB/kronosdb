# System events (`$` namespace)

KronosDB stores its own internal state as events in the same log it
serves to applications. A **system event** is a durable fact about the
store itself — a schedule was created, a transformation completed — as
opposed to a fact about the application's domain.

The reasoning is the store's own thesis turned inward: almost any
stateful subsystem can be event sourced, and the log is already the one
thing that is quorum-durable, byte-exact replicated (ADR-0001),
archived (ADR-0002), and crash-recoverable. A subsystem that keeps its
state as events inherits all of it. A subsystem that keeps state
anywhere else — a side table, a control-plane entry, a file — is a
second durability domain that must reimplement every one of those
properties and will do it worse.

## Rules

**1. Server-authored only.** System events are appended exclusively by
the server. Client appends carrying a `$`-prefixed event type or tag
key are rejected. Provenance is the definition: a `$` event means *the
system wrote this*.

**2. Invisible to clients.** `Source`, `Subscribe`, and `Tags` never
return system events, and never match system tags — there is no opt-in
flag. System tags attached to user events are stripped from results and
unmatchable by client conditions. There are two read paths: the
internal one, which sees everything, and the client one, which sees
only user events. Applications reach system state through purpose-built
APIs (`ListSchedules`) and the admin console.

This is the rule that makes the pattern safe to use aggressively.
Invisible state is *private implementation detail*: no client can build
a projection on it, so we are free to add, reshape, and retire system
event types at will. If they were visible — even opt-in — every type
would become permanent public API on first use. The API is the
contract, not the log.

**3. Semantic facts, not telemetry.** A system event records a state
change an operator would want to source a year later: "context
created", "transformation completed", "schedule cancelled". Never
heartbeats, per-request metrics, replication progress, or cache
statistics. Those are metrics; they belong in `/metrics`. The test:
*would you want to replay this to reconstruct what the system was
doing?* If the answer is "no, I want to graph it", it is not a system
event.

**4. Never circular.** Nothing the log's own durability machinery
depends on may be a system event: watermark movement, leader claims,
membership, segment seals, replication state. That machinery must
function *before* the log is readable, so it lives in the Raft control
plane (ADR-0001) or in segment headers. System events sit strictly
above replication and may only depend on it, never the reverse.

**5. Not transformable.** Event transformations apply to user events
only; system events pass through untouched. A rewrite of history must
not be able to rewrite the record of rewrites.

**6. They consume positions, but never inflate the head clients see.**
System events occupy log positions like any other event, so client reads
observe gaps. That much is already normal: any filtered `Source` returns
non-contiguous positions, so consumers must advance a cursor by the
positions they actually receive rather than by counting.

What is *not* acceptable is a head a client can never reach. If `GetHead`
returned the true head, a fully drained consumer would sit permanently
short of it — reporting phantom lag that grows over time, and hanging any
poll that waits to catch up. So `GetHead` returns the **visible head**:
the position just past the last readable event. `head` stays the true
head internally, where the watermark, read bounds, and DCB markers all
need every position counted.

This is why the marker tag exists and why `append_system` stamps it
rather than trusting callers. The read path judges visibility by event
type; the visible head is resolved from the index by marker tag. A system
event carrying only one of the two would be unreadable yet still counted,
which is exactly the phantom lag the visible head exists to prevent.

A parallel system-only log was rejected: it would duplicate the writer,
watermark, replication, and archival machinery to avoid a problem that a
second watermark solves.

## Registry

Every system event type is listed here. The registry is internal
documentation, not a compatibility promise (rule 2).

| Type | Owner | Status | Meaning |
|---|---|---|---|
| `$schedule.created` | ADR-0003 | Implemented | An event was scheduled for future append |
| `$schedule.rescheduled` | ADR-0003 | Proposed | A live schedule's due time or payload changed |
| `$schedule.cancelled` | ADR-0003 | Implemented | A live schedule was cancelled without firing |
| `$schedule.fired` | ADR-0003 | Implemented | A schedule resolved by firing; written atomically with the target event |
| `$schedule.superseded` | ADR-0003 | Proposed | A schedule's fire-time condition failed; resolved without firing |
| `$transformation.started` | ADR-0004 | Proposed | A transformation job began, with its function spec and scope |
| `$transformation.rewritten` | ADR-0004 | Proposed | One segment was materialized at a new revision (carries blake3) |
| `$transformation.completed` | ADR-0004 | Proposed | Every segment in scope was rewritten |
| `$transformation.cancelled` | ADR-0004 | Proposed | A job was abandoned; orphan revisions are reaped |

Every system event also carries the marker tag that makes it invisible;
`append_system` stamps it, so it is never a caller's responsibility.
Subsystems correlate their own events with their own tags:
`$schedule:{token}` and `$transformation:{id}`.

System events may carry **only** `$` tags — `append_system` rejects a
user-namespace tag on a `$`-typed event. DCB checks match by tag with no
visibility filter, so a system event carrying `orderId=X` would trip a
client's consistency condition on a conflict the client can neither see
nor explain. A user event that needs correlating to system state gets a
`$` tag (the fired event's `$schedule:{token}`), never the reverse.

## Adding a type

1. Check it against the six rules — particularly 3 (semantic, not
   telemetry) and 4 (not circular).
2. Name it `$subsystem.past-tense-fact`, lowercase, dotted.
3. Add it to the registry above with its owning ADR.
4. Expose whatever applications legitimately need through a purpose-built
   API, never by making the event visible.
