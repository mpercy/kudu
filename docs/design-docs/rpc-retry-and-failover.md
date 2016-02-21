<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Introduction

Kudu has many RPCs that need error handling. When an RPC fails due to a timeout
or network failure, the sender needs to retry it in the same or in a different
server, but it often doesn't know whether the original RPC succeeded. Certain
RPCss are not idempotent i.e. if handled twice they will lead to incorrect state
and thus we need a mechanism by which the sender can retry the RPC but the receiver
is guaranteed to execute it **Exactly Once**.

Kudu currently has the following RPC service interfaces: TabletServer,
TabletServerAdmin, Master, Consensus, RemoteBootstrap and Generic.
RPCs can be classified according to their semantics requirements in:

1. Doesn't need Exactly Once - Operations that don't mutate server state.

2. Would benefit from Exactly Once - Operations that only mutate non-persistent state or mutate
state that is not replicated across servers and that would benefit from exactly once for
generalized error handling, but that don't strictly need it.

3. Needs Exactly Once but has an alternative - Operations like in 2 but that already
have some alternative error handling mechanism like CAS.

4. Require Exactly Once for correctness - Operations that don't currently have E.O. implemented
and require it for correctness.

An appendix at the end of this document classifies all the RPCs in these categories.

This documents presents a plan to tackle the operations that fall in 4 first, but in such
a way that we can also address those that fall in 2 and 3 later.

# Problem breakdown

[1] introduces 4 subproblems that need to be solved to obtain exactly-once semantics on
replicated RPCs, we'll use the same terminology to avoid redefining the concepts:

- Identification - Each RPC must have a unique identifier.
- Completion Record - Each RPC must have a durable record of its completion.
- Retry rendezvous - When an RPC is retried it must find the completion record of the
                      previous attempt.
- Garbage collection - After a while completion records must be garbage collected.

## Design options

We can address these problems in multiple ways and, in particular, at multiple abstraction
layers:

1. Completely encapsulated at the RPC layer - This is the option that [1] presents at length,
allowing to "bolt-on" these properties almost independently of what the underlying RPCs
are. This option, while very general, would require a lot of work particularly as for
replicated RPCs we'd need to come up with a generic, durable, Completion Record mechanism.
While "elegant" in some ways this option also seems weird in other ways: for instance
it's not totally clear what happens when an RPC mutates multiple "server objects" or
what these server objects really are.

2. Handled ad-hoc by the client and the tablet server/master, outside of the RPC layer -
With this option, we choose to handle errors at the TabletServer/TabletPeer, ad-hoc, and for
each different RPC. For specific RPCs like Write() this option seems to map to existing
components quite well. For instance the completion record can be the raft log and replica
replay would mean that it would be relatively easy to implement a retry rendezvous
mechanism. However this option lacks in generality and it would likely require duplicate
error handling for operations that are not tablet server transactions.

3. A hybrid of the above - Certain parts of the problem would be handled by the RPC layer,
such as retrying and retry rendez-vous logic, but the other components would be provided,
through a common interface, by other layers. For instance the RPC layer would know how to
sequence and retry RPCs but would rely on a pluggable Completion Record and Retry rendezvous.
We'd basically have three types of implementation for the Completion Record component of
replay cache:
- No Op - A passthrough, no-op component that just delegates to what handling we already
have.
- Generic non-persistent - To ease in error handling and for Cat 2 operations in general
we could have a generic version of this component that just caches previous replies in-
memory.
- Ad-hoc - A tailored version of this component for certain RPCs. We would, for instance
create a version of this that integrates with consensus and works with replication.

# Discussion and mapping to current components

Option 1 is reasonably well presented in [1], so the discussion will mostly focus on options
2 and 3.

Kudu already has mechanisms in place that help with several of subproblems we're trying to address,
we should reuse these when possible. In general terms, here's how these problems map to Kudu's design:

- Identification - Although the clients don't currently have an ID, a natural place to issue
one would be when the client first reaches out to the master asking for the tablet locations,
to fully guarantee unique IDs though, we'd have to store client IDs in the master tablet.
Tablet operations need a sequence number. Sequence numbers need to be per-tablet unique, since
different tablets are replicated to different tablet servers.

Note on client IDs: [1] discusses client leases at length and why they are a useful feature
(and even required in some cases, to guarantee global uniqueness and to help with Completion
Record garbage collection). In particular the paper mentions that leases have ids that
are issued by a centralized LeaseManager. The client's lease ID together with a sequence number
is what identifies each single RPC uniquely. On top of a lease issuing mechanism this system
also requires a lease verification mechanism, in which tablet servers verify leases with the
LeaseManager. While this option seems attractive to handle clients that are partitioned from
the cluster for a period longer than the garbage collection period and, in particular to handle
two phase commit, I don't think we need it from the get go and that we can "manage" with simple
client IDs (e.g. uuids) generated locally for Milestone 1. However if we ever plan to support
a large number of clients in the future ([1] mentions 1 million clients) then uuids might become
problematic, due to id collisions. Clients leases however, is something that can be added later
without significant rework, and would be something left out of the first Milestone.

- Completion Record - For replicated requests, like write requests, each client request is
recorded by the consensus log almost as is so it wouldn't be problematic to additionally store
the client id and request seq no. so when a write is "consensus committed" all future handlers
of that write (future leaders) will automatically be able to identify the client and request.
This would, of course, limit exactly once semantics on server failover to RPCs who end up as
consensus rounds.
For non-replicated requests, depending on the operation itself, the server could either use
a generic system to handle completion records (non-persistent), or could have some specific
implementation for particular cases.

- Retry rendezvous - For this element we'd need to create a new component, the Replay Cache,
which basically keeps a set of past completion records, and their current status, so that
when a request is retried we'd be able to identify it as a retry request, instead of a new
one. The Replay cache would then use the status to either:
  - Reply immediately - If the original operation was completed the client would then receive
    a response immediately with the original status. Note that this means we need to keep
    all the information that we usually include in responses, like the per-row errors, in
    this cache.
  - Attach to the ongoing operation - If the original operation has not been completed at
    the time of the retry, then the retry should be attached to the ongoing transaction.
    This "attachment" means basically registering a callback that will be called by the
    executing thread, on completion, additionally replying to the new RPC.

- Garbage collection - If clients IDs are leases, then garbage collection happens naturally
when either the client lease expires or the client has marked an operation as completed.
If client IDs are just UUIDs garbage collection can occur based on a time policy
or memory pressure policy (HDFS has time and occupied memory percentage as policies).

# Possible implementation plan and milestones

The design proposal that follows aims at ultimately ending up with Design Option 2, where
we handle identification and client retry logic centrally at the RPC layer but have pluggable
interfaces, implemented by other components, from which to fetch/store completion records
or attach retries to ongoing operations. However, at the cost of some rework, we prefer to
start with Design Option 3, for Write() operations only, and to set milestones that include
some useful functionality (versus building each individual component of the full design
that would not work by itself).

## Milestone 1
Define interfaces for Completion Record storage and handling and implement
Exactly Once semantics for tablet writes, with ad-hoc error/retry handling and simple
client id issuing.

We'll start by solving the exactly once problem for the TabletServer Write() rpc (the only
cat. 4 operation), which is arguably the most important one since it's the one that happens
the most often and can lead to data corruption. We'll define a
CompletionRecordStore interface which will be able to:
1. Start new operations if this is a new RPC.
2. For RPCs that have previously completed it will be able to fetch existing completion
    records and reply to the caller immediately.
3. For operations that are still ongoing, it will attach the new RPC to the ongoing
    operation.

All replicas will maintain CompletionRecordStore component and will update it each time
they receive an operation from the client (if Leader) or from the leader (if Follower).
On replay, replicas will populate this component with the operations read from the
wal. Note that this component must cache not only the RPC "result" (i.e. SUCCESS/ABORTED
etc..) but also the per-row errors, if any, that were found when executing the original RPC.

Retry handling on the client side and retry rendez-vous logic will be implemented at the
KuduSession and TabletPeer components respectively, at this stage.

In this stage we'd implement a simple garbage collection policy based on time.

Client ID issuing is done locally, by an implementation of LeaseManager that just generates
a UUID.

## Milestone 2
Push client retry logic to the RPC system. Address all cat 2 RPCs. Implement space
based garbage collection.

This would move the retry logic implemented specifically for Write() in the previous milestone
into the RPC (client layer), but would keep the server side ad-hoc. Cat 2 RPCs would get
their own CompletionRecordStore and have it be integrated with the retry rendezvous
mechanism.

## Milestone 3
Push server rendezvous logic into the RPC system. Come up with a generic and a no-op
CompletionRecordStore.

Each RPC would have registered a CompletionRecordStore. Certain replicated RPCs like
Write() would have the one implemented in Milestone 1 that is able to handle cross-tablet
server retries, some RPC's for which retries are already handled somehow would have a no-op
CompletionRecordStore and some RPC's would have a "local" version of the CompletionRecordStore.
This local version would not have a durability mechanism and would be local to each tablet server,
and would be totally generic, i.e. it would be the same for all the RPCs. This would not help
with retries across TabletServer reboots but it would help immensely in consoldating retry
logic that we have spread all over.

Milestone 4 - Client leases and coordinated garbage collection.

In this final stage we will implement the client lease system suggested in [1] where client
leases are stored and garbage collection is performed with the help with a "cluster clock".

Note: from [1] a cluster clock is "a time value that increases monotonically at the rate
of the lease server’s real-time clock and is durable across lease server crashes.". It
seems that our HybridClock would provide the same guarantees.

## Appendix - RPC Classification

The following is a list the rpcs that mutate state and their requirements:

```
void TabletServerServiceIf::Write();
```

Write() falls into cat. 4. Calls to this rpc are sent from clients, replicated by servers
and may fail for many reasons, also if a retry request fails, its impossible to distinguish
if it's because a prior attempt succeeded, or because it couldn't succeed due to prior
state.

```
void TabletServerAdminServiceIf::CreateTablet();
void TabletServerAdminServiceIf::DeleteTablet();
```

These operations are called by the master and already handle at least once. They might benefit
from having a common retry mechaninism (e.g. have the RPC client perform the retries) but
it isn't strictly necessary, thus these are cat. 3.

```
void TabletServerAdminServiceIf::AlterSchema();
```

AlterSchema() is issued by the master and already has some error handling mechanism embedded,
so it would initially fall into cat.3.

```
void ConsensusServiceIf::UpdateConsensus();
```

UpdateConsensus() is already naturally idempotent, due to the deduplication that happens
inside consensus (we actually do better here, since when we retry we might send more data).
So we can classify it as cat. 3.

```
void ConsensusServiceIf::ChangeConfig();
```

Change config already has a CAS mechanism, so it falls into cat. 3 but could eventually
be migrated to using the same retry logic as the remaining operations.

```
void ConsensusServiceIf::StartRemoteBootstrap();
```

Same as Create/DeleteTablet();

```
void MasterServiceImpl::CreateTable();
void MasterServiceImpl::DeleteTable();
void MasterServiceImpl::AlterTable();
```
Create/Delete/Alter table operations are replicated and, although errors can usually
be handled gracefully on the client side, they would benefit from E. O. so they fall into
cat 2.


### References

[1](web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf "Implementing Linearizability at Large Scale and Low Latency")
