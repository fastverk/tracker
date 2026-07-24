# tracker

Generic work-tracker contract (`TrackerService`) + provider adapters.

The operations a *work tracker* (Linear, Jira, GitHub/GitLab issues, …) provides
that an autonomous agent fleet needs, modeled **once** as a proto service so a
single contract covers every provider:

- read a backlog, and read one item in full;
- learn the workspace's projects and workflow states;
- move an item through those states, comment on it, and link the change that
  resolves it.

`proto/tracker/v1/tracker.proto` defines the gRPC `TrackerService`; in-process
consumers use the hand-written async `Tracker` trait whose methods take/return
the same proto messages (`TrackerRef`, `WorkItemRef`, `WorkItem`,
`WorkItemState`, `WorkItemQuery`). `LinearTracker` (GraphQL) implements it.

This is the deliberate sibling of [`forge`](../forge): **forge is "where the code
lives", tracker is "where the work is tracked"**. Same layout, same conventions,
same conformance-suite discipline — an adapter author learns the pattern once.

## Why a separate contract from `forge.v1`

`forge.v1.ForgeDiscovery` already lists GitHub and GitLab issues, so folding
trackers into it looks cheaper. It isn't:

- a tracker has concepts a code host does not — workflow states, projects,
  cycles, estimates, sub-items — and bending Linear into `forge.v1.Issue` (a
  seven-field discovery row) loses exactly the fields a campaign needs;
- `forge.v1.Issue` has no write surface at all, and the flywheel is not
  read-only: it must move an item to *In Progress*, comment, and link the PR;
- keeping them separate makes GitHub and GitLab issues **one adapter here**
  rather than the model every other provider is bent to fit.

## Credentials

No service in this repo holds a tracker credential. `tracker-gateway` builds a
per-request adapter from the caller's gRPC metadata
(`x-fastverk-linear-token`), exactly as `forge-gateway` takes
`x-fastverk-gitlab-token`, so every operation runs as the caller and adding a
provider never means giving a shared daemon another standing secret.

## What is deliberately NOT here

Routing a work item to a **repository** (and to a Bazel blast radius) is not a
tracker operation — no provider knows it. It is fleet policy, expressed by the
caller: a campaign's routing table, or a triage agent. `WorkItem` carries the
*evidence* a router needs (labels, project, group, free text) and stops there.

## The conformance suite is the contract

`tracker::conformance` is a library module behind the non-default `testing`
feature, not a `tests/` file — so an adapter in its own Bazel module runs the
**identical** battery instead of copying it. An adapter integrates in one line:

```rust,ignore
tracker::conformance_suite!(my_adapter, MyFixture::new());
```

The cases pin behavior, not shape: transitions are idempotent, keyed comments
dedupe, links dedupe by URL, paging terminates without duplicating, `get`
returns the full body, an item resolves by human key alone, and — the most
consequential one — **a filter that matches nothing returns nothing**. An
adapter that silently drops a filter it cannot express does not return slightly
wrong results; it returns the entire backlog and a campaign dispatches an agent
at every row.

Correspondingly, a surface an adapter cannot serve must **fail loudly**. The
Linear adapter declares `text_search: false` and rejects a text query rather
than answering with the unfiltered backlog.

## Build

`.bazelrc`:

```
common --registry=https://registry.fastverk.com/
common --registry=https://bcr.bazel.build/
```

`MODULE.bazel`:

```python
bazel_dep(name = "tracker", version = "0.0.1")
```

Then depend on `@tracker//:tracker` from a `rust_library`.

```sh
bazel build //:tracker //:tracker-gateway
bazel test  //:tracker_test //:conformance
```

The OCI image needs a linux target platform — `gcr.io/distroless/cc-debian12`
publishes no darwin variant, so a bare `bazel build //...` fails at analysis on
a Mac. Build it the way the RBE does (same as the sibling `forge` module):

```sh
bazel build //:tracker-gateway-image --platforms=//tools/oci:linux_amd64
bazel build //:tracker-gateway-image_push --config=rbe          # what CI runs
```

Bazel is the only build system here. The crate also carries a `Cargo.toml`
because `crate_universe` resolves dependencies from it — not as a second build
path.

## Run

```sh
TRACKER_GATEWAY_BIND=0.0.0.0:50068 tracker-gateway
```

Callers pass their own credential per request:

```sh
grpcurl -plaintext \
  -H "x-fastverk-linear-token: $LINEAR_API_KEY" \
  -d '{"tracker":{"tracker":"TRACKER_LINEAR"},"query":{"projects":["DEV"],"state_categories":["STATE_CATEGORY_TODO"],"labels":["agent/ready"]}}' \
  localhost:50068 tracker.v1.TrackerService/ListWorkItems
```

## Adding a provider

1. A new `Tracker` enum value in `tracker.proto` — **append only**, never
   renumber; consumers persist these values.
2. An adapter module implementing `Tracker`, declaring honest `capabilities()`.
3. A fixture + `conformance_suite!` invocation.
4. A metadata key and an arm in `gateway::TrackerGateway::adapter`. Unknown
   values are rejected there, never defaulted — `forge.v1` learned that the hard
   way when "not GitHub therefore GitLab" became a wrong-API write.
