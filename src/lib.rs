//! Generic work-tracker contract + provider adapters.
//!
//! [`Tracker`] is the set of operations an autonomous agent fleet needs from a
//! work tracker: read a backlog, read one item in full, learn the workspace's
//! projects and workflow states, move an item through those states, comment on
//! it, and link the change that resolves it. The DTOs ([`WorkItemRef`],
//! [`WorkItem`], [`WorkItemState`], [`WorkItemQuery`]) are proto messages
//! (package `tracker.v1`); the generated gRPC `TrackerService` is the same
//! contract for the tracker-gateway daemon. In-process consumers use the async
//! [`Tracker`] trait directly.
//!
//! This is the deliberate sibling of the `forge` crate — `forge` is "where the
//! code lives", `tracker` is "where the work is tracked" — and follows its
//! layout exactly, so an adapter author learns the pattern once.
//!
//! # Credentials
//!
//! No type in this crate stores a credential beyond the lifetime of the adapter
//! the caller constructed. [`gateway::TrackerGateway`] holds none at all: it
//! builds a per-request adapter from the caller's gRPC metadata, so every
//! operation runs as the caller. Adding a provider never means handing a shared
//! daemon another standing secret.

use async_trait::async_trait;

pub mod pb {
    //! Generated `tracker.v1` proto types + gRPC service stubs.
    tonic::include_proto!("tracker.v1");
}

pub mod gateway;
pub mod linear;

// Test doubles for the `Tracker` contract. A real module behind a non-default
// feature, NOT `#[cfg(test)]`: an adapter living in its own Bazel module cannot
// see this crate's test-only code. Same reasoning that made `TrackerError`
// concrete rather than `anyhow`.
#[cfg(feature = "testing")]
pub mod testing;

// The conformance suite — the executable specification every adapter is held to.
// Also behind `testing`, and for the same reason one level up: Rust integration
// tests are not importable by other crates, so an out-of-crate adapter could not
// run the shared battery and would copy it instead. A copy proves nothing about
// the shared contract, because a copy drifts.
#[cfg(feature = "testing")]
pub mod conformance;

pub use pb::{
    Priority, Project, StateCategory, Tracker as TrackerKind, TrackerCapabilities, TrackerRef,
    WorkItem, WorkItemQuery, WorkItemRef, WorkItemState,
};

/// A tracker operation error. A concrete type (not `anyhow`) so the public
/// [`Tracker`] API doesn't leak `anyhow` — which also lets consumers in a
/// *different* crate universe (a separate Bazel module) implement and call the
/// trait without the two `anyhow` instances colliding. Adapters build it from
/// their internal `anyhow` errors via `From`; consumers turn it back into their
/// own error type (it's a `std::error::Error`, so `anyhow`'s blanket `?` just
/// works).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TrackerError(String);

impl TrackerError {
    /// A `TrackerError` from any displayable message.
    pub fn msg(m: impl std::fmt::Display) -> Self {
        Self(m.to_string())
    }
}

impl From<anyhow::Error> for TrackerError {
    fn from(e: anyhow::Error) -> Self {
        Self(format!("{e:#}"))
    }
}

/// `Result` for [`Tracker`] operations.
pub type TrackerResult<T> = Result<T, TrackerError>;

/// One page of a backlog listing.
#[derive(Debug, Clone, Default)]
pub struct WorkItemPage {
    pub items: Vec<WorkItem>,
    /// Empty when the backlog is exhausted.
    pub next_page_token: String,
}

/// Where [`Tracker::transition`] should move an item.
///
/// The [`Self::Category`] form is what a fleet controller uses: it wants "in
/// progress" without learning each workspace's vocabulary.
#[derive(Debug, Clone)]
pub enum TransitionTarget {
    /// An explicit provider state id, from [`Tracker::list_states`].
    StateId(String),
    /// A normalized category, resolved by the adapter against the item's own
    /// project workflow.
    Category(StateCategory),
}

/// Outcome of [`Tracker::transition`] — idempotent over an item already in the
/// target state.
#[derive(Debug, Clone)]
pub struct Transitioned {
    /// The state the item is in AFTER the call.
    pub state: WorkItemState,
    /// False when the item was already there.
    pub changed: bool,
}

/// Outcome of [`Tracker::comment`] — idempotent under an `idempotency_key`.
#[derive(Debug, Clone)]
pub struct PostedComment {
    pub id: String,
    pub url: String,
    /// False when an existing comment with the same key was returned.
    pub created: bool,
}

/// Outcome of [`Tracker::link_change`] — idempotent by URL.
#[derive(Debug, Clone)]
pub struct LinkedChange {
    pub id: String,
    /// False when the link already existed.
    pub created: bool,
}

/// A change to link onto a work item.
#[derive(Debug, Clone, Default)]
pub struct ChangeLink {
    /// Web URL — the only universally linkable handle, and the idempotency key.
    pub url: String,
    /// Forge ref ("fastverk/plugin-tbzl#5", "aion/web!75") when known.
    pub change_ref: String,
    /// Human title for the link.
    pub title: String,
}

/// Convenience: the human key for an item ("DEV-18395"), falling back to the
/// opaque provider id when an adapter left the key empty.
///
/// Adapters MUST populate `key`; this exists so a log line or a branch name
/// never renders as an empty string when one does not.
#[must_use]
pub fn item_slug(item: &WorkItemRef) -> String {
    if item.key.is_empty() {
        item.id.clone()
    } else {
        item.key.clone()
    }
}

/// The branch name a provider expects for its own change-linking to fire.
///
/// Prefers the provider's own `git_branch_name` (Linear's `gitBranchName`), and
/// otherwise derives the lowercase-key form Linear itself uses ("DEV-18395" →
/// "dev-18395"). Load-bearing: an agent that names its branch anything else gets
/// an attachment at best and no state transition at all.
#[must_use]
pub fn branch_name_for(item: &WorkItem) -> String {
    if !item.git_branch_name.is_empty() {
        return item.git_branch_name.clone();
    }
    match &item.r#ref {
        Some(r) => item_slug(r).to_lowercase(),
        None => String::new(),
    }
}

/// The operations a work tracker (Linear, Jira, forge issues, …) provides to an
/// agent fleet.
///
/// The write methods are idempotent, so a resuming reconcile loop can call them
/// repeatedly without duplicating state. An adapter that cannot support a method
/// must fail loudly (via the default implementations below, which return an
/// explicit "unsupported" error) rather than silently return a plausible
/// default — and must say so in [`Tracker::capabilities`].
#[async_trait]
pub trait Tracker: Send + Sync {
    /// Which tracker this adapter targets.
    fn kind(&self) -> TrackerKind;

    /// One page of the backlog matching `query`.
    ///
    /// An adapter MUST apply every filter it claims in [`Self::capabilities`],
    /// server-side where the provider allows and in-process otherwise. Silently
    /// ignoring a filter turns a campaign into unbounded fan-out over an entire
    /// backlog, so an adapter that cannot apply a set field MUST fail rather
    /// than over-return.
    async fn list_work_items(
        &self,
        tracker: &TrackerRef,
        query: &WorkItemQuery,
        page_size: i32,
        page_token: &str,
    ) -> TrackerResult<WorkItemPage>;

    /// One item, with its full `description`.
    async fn get_work_item(&self, item: &WorkItemRef) -> TrackerResult<WorkItem>;

    /// The workspace's projects (Linear teams, Jira projects, forge repos).
    async fn list_projects(&self, tracker: &TrackerRef) -> TrackerResult<Vec<Project>>;

    /// The workflow states, optionally scoped to one project. Providers with
    /// per-project workflows may return same-named states with distinct ids.
    async fn list_states(
        &self,
        tracker: &TrackerRef,
        project: &str,
    ) -> TrackerResult<Vec<WorkItemState>>;

    /// Move an item. Idempotent: an item already in the target state returns
    /// `changed: false`, not an error.
    async fn transition(
        &self,
        _item: &WorkItemRef,
        _target: &TransitionTarget,
    ) -> TrackerResult<Transitioned> {
        Err(TrackerError::msg("transitions unsupported by this adapter"))
    }

    /// Comment on an item. When `idempotency_key` is non-empty, an adapter that
    /// finds an existing comment carrying it MUST return that one rather than
    /// post a second.
    async fn comment(
        &self,
        _item: &WorkItemRef,
        _body: &str,
        _idempotency_key: &str,
    ) -> TrackerResult<PostedComment> {
        Err(TrackerError::msg("comments unsupported by this adapter"))
    }

    /// Attach a change (PR/MR) to an item. Idempotent by `link.url`.
    async fn link_change(
        &self,
        _item: &WorkItemRef,
        _link: &ChangeLink,
    ) -> TrackerResult<LinkedChange> {
        Err(TrackerError::msg("change links unsupported by this adapter"))
    }

    /// What THIS adapter answers for — not what the product advertises. Ask
    /// before attempting an optional surface.
    async fn capabilities(&self) -> TrackerResult<TrackerCapabilities> {
        Ok(TrackerCapabilities::default())
    }
}
