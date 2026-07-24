//! The [`Tracker`] conformance suite — the executable specification.
//!
//! # What this is for
//!
//! The [`Tracker`] trait states its guarantees in prose: "the write methods are
//! idempotent, so a resuming reconcile loop can call them repeatedly without
//! duplicating state". Prose does not fail a build.
//!
//! **This suite is the contract.** An adapter is correct when it passes. New
//! contract semantics get a case here *first*; they are not asserted in an
//! adapter-local test, because a test that lives next to one adapter cannot
//! constrain the others.
//!
//! # Why this lives in `src/`, not `tests/`
//!
//! Rust integration tests are not importable by other crates, so an out-of-crate
//! adapter — a Jira adapter in its own repo, say — could not run the shared
//! battery and would copy it instead. A copy drifts, and an adapter that passes
//! its own copy has proved nothing about the shared contract. So the suite is a
//! library module behind the non-default `testing` feature, the same reasoning
//! that put [`crate::testing::FakeTracker`] there.
//!
//! # How to run it against a new adapter
//!
//! ```rust,ignore
//! use tracker::conformance::Fixture;
//!
//! struct MyFixture { /* connection state */ }
//! impl Fixture for MyFixture { /* tracker(), tracker_ref(), project(), scratch_item() */ }
//!
//! tracker::conformance_suite!(my_adapter, MyFixture::new());
//! ```
//!
//! That single invocation is the whole integration. An adapter that cannot
//! support a method should fail loudly here rather than silently returning a
//! plausible default — see [`unsupported_is_explicit`].
//!
//! Two requirements on the consumer: depend on `tracker` with the `testing`
//! feature enabled, and have `tokio` available with the `macros` and `rt`
//! features — the generated cases are `#[tokio::test]`, so the attribute is
//! resolved in the CONSUMER's crate, not this one.

use crate::{ChangeLink, StateCategory, Tracker, TrackerRef, TransitionTarget, WorkItemQuery,
            WorkItemRef};

/// A label no workspace will have. Used to prove a filter is really applied:
/// every fixture can guarantee zero matches for it, with nothing to seed.
const ABSENT_LABEL: &str = "fastverk-conformance-absent-label";

/// Builds a live adapter plus one work item it may mutate.
///
/// A trait rather than a closure so a real-adapter fixture can hold connection
/// state and clean the item up on drop.
pub trait Fixture {
    /// The adapter under test.
    fn tracker(&self) -> &dyn Tracker;
    /// The instance `scratch_item` lives in.
    fn tracker_ref(&self) -> TrackerRef;
    /// A project whose workflow has states in at least the Todo, InProgress and
    /// Done categories.
    fn project(&self) -> String;
    /// A work item the suite may MUTATE — transition, comment, link. It must
    /// already exist and belong to `project()`.
    fn scratch_item(&self) -> WorkItemRef;
}

// ── the cases ───────────────────────────────────────────────────────────────

/// `transition` is idempotent: moving an item to the state it is already in
/// reports `changed: false`, it does not error.
///
/// This is the single most load-bearing guarantee in the trait. A campaign
/// controller re-reconciles on every watch event and re-drives every step; an
/// adapter that errors here wedges the campaign on its second pass.
pub async fn transition_is_idempotent(fx: &dyn Fixture) {
    let t = fx.tracker();
    if !t.capabilities().await.unwrap().transitions {
        return;
    }
    let item = fx.scratch_item();
    let target = TransitionTarget::Category(StateCategory::InProgress);

    let first = t.transition(&item, &target).await.unwrap();
    assert_eq!(
        first.state.category,
        StateCategory::InProgress as i32,
        "a transition must land in the requested category"
    );

    let second = t.transition(&item, &target).await.unwrap();
    assert!(
        !second.changed,
        "re-transitioning to the current state MUST report changed=false, not error"
    );
    assert_eq!(
        first.state.id, second.state.id,
        "an idempotent transition must report the SAME state"
    );
}

/// A category target resolves against the item's OWN project workflow, and the
/// resolved state is reported back.
///
/// Callers ask for "in progress" without knowing the workspace's vocabulary, so
/// the adapter must both resolve it and say what it resolved to — a caller that
/// cannot see the chosen state cannot tell a correct resolution from a silent
/// wrong-board one.
pub async fn category_target_resolves_and_is_reported(fx: &dyn Fixture) {
    let t = fx.tracker();
    if !t.capabilities().await.unwrap().transitions {
        return;
    }
    let item = fx.scratch_item();

    let done = t
        .transition(&item, &TransitionTarget::Category(StateCategory::Done))
        .await
        .unwrap();
    assert_eq!(done.state.category, StateCategory::Done as i32);
    assert!(
        !done.state.id.is_empty(),
        "the resolved state must carry the provider's own id"
    );
    assert!(
        !done.state.name.is_empty(),
        "the resolved state must carry the workspace's own name"
    );

    let states = t
        .list_states(&fx.tracker_ref(), &fx.project())
        .await
        .unwrap();
    assert!(
        states.iter().any(|s| s.id == done.state.id),
        "the resolved state must belong to the project's own workflow"
    );
}

/// `comment` with an idempotency key posts once and returns the existing comment
/// on every later call.
///
/// A crash-looping reconcile posting "agent started work" forty times is the
/// failure this pins.
pub async fn comment_is_idempotent_under_a_key(fx: &dyn Fixture) {
    let t = fx.tracker();
    if !t.capabilities().await.unwrap().comments {
        return;
    }
    let item = fx.scratch_item();
    let key = "conformance-comment-key";

    let first = t.comment(&item, "hello from conformance", key).await.unwrap();
    assert!(first.created, "the first keyed comment should be created");

    let second = t.comment(&item, "hello from conformance", key).await.unwrap();
    assert!(
        !second.created,
        "a repeat keyed comment MUST return the existing one, not post again"
    );
    assert_eq!(
        first.id, second.id,
        "an idempotent comment must return the SAME comment"
    );
}

/// Without a key there is no deduplication — and that is contract, not a bug.
///
/// Stated explicitly so a caller knows the key is REQUIRED for retry safety
/// rather than a nicety, and so no adapter invents content-hash dedupe that
/// would silently swallow a legitimate repeated note.
pub async fn comment_without_a_key_is_not_deduped(fx: &dyn Fixture) {
    let t = fx.tracker();
    if !t.capabilities().await.unwrap().comments {
        return;
    }
    let item = fx.scratch_item();

    let first = t.comment(&item, "unkeyed note", "").await.unwrap();
    let second = t.comment(&item, "unkeyed note", "").await.unwrap();
    assert!(first.created && second.created);
    assert_ne!(
        first.id, second.id,
        "unkeyed comments must NOT be deduplicated"
    );
}

/// `link_change` is idempotent by URL.
pub async fn link_change_is_idempotent_by_url(fx: &dyn Fixture) {
    let t = fx.tracker();
    if !t.capabilities().await.unwrap().link_changes {
        return;
    }
    let item = fx.scratch_item();
    let link = ChangeLink {
        url: "https://forge.invalid/fastverk/plugin-tbzl/pull/5".to_string(),
        change_ref: "fastverk/plugin-tbzl#5".to_string(),
        title: "test(mcp): cover the arg extractors".to_string(),
    };

    let first = t.link_change(&item, &link).await.unwrap();
    assert!(first.created);

    let second = t.link_change(&item, &link).await.unwrap();
    assert!(
        !second.created,
        "re-linking the same URL MUST report the existing link, not duplicate it"
    );
    assert_eq!(first.id, second.id);
}

/// A set query filter is APPLIED, never silently ignored.
///
/// The most consequential case in the suite. A campaign turns a query into
/// fan-out; an adapter that drops a filter it cannot express does not return
/// slightly wrong results, it returns the ENTIRE backlog and dispatches an agent
/// at every row. So: an impossible label must yield nothing.
pub async fn query_filters_narrow_and_never_over_return(fx: &dyn Fixture) {
    let t = fx.tracker();
    let tr = fx.tracker_ref();
    if !t.capabilities().await.unwrap().labels {
        return;
    }

    let scoped = WorkItemQuery {
        projects: vec![fx.project()],
        ..Default::default()
    };
    let all = t.list_work_items(&tr, &scoped, 50, "").await.unwrap();
    assert!(
        !all.items.is_empty(),
        "the fixture's project must contain at least the scratch item"
    );

    let impossible = WorkItemQuery {
        projects: vec![fx.project()],
        labels: vec![ABSENT_LABEL.to_string()],
        ..Default::default()
    };
    let none = t.list_work_items(&tr, &impossible, 50, "").await.unwrap();
    assert!(
        none.items.is_empty(),
        "a label filter that matches nothing MUST return nothing — returning \
         {} items means the filter was dropped, which turns a campaign into \
         fan-out over the whole backlog",
        none.items.len()
    );
}

/// Paging terminates, and pages neither duplicate nor omit.
pub async fn listing_pages_and_terminates(fx: &dyn Fixture) {
    let t = fx.tracker();
    let tr = fx.tracker_ref();
    let q = WorkItemQuery {
        projects: vec![fx.project()],
        ..Default::default()
    };

    let mut seen: Vec<String> = Vec::new();
    let mut token = String::new();
    // Bounded so a non-terminating adapter fails the case instead of the run.
    for _ in 0..50 {
        let page = t.list_work_items(&tr, &q, 1, &token).await.unwrap();
        for it in &page.items {
            let key = crate::item_slug(&it.r#ref.clone().unwrap_or_default());
            assert!(
                !seen.contains(&key),
                "item {key} was returned on two pages — paging must not duplicate"
            );
            seen.push(key);
        }
        if page.next_page_token.is_empty() {
            token.clear();
            break;
        }
        assert_ne!(
            page.next_page_token, token,
            "the continuation token must advance, or paging never terminates"
        );
        token = page.next_page_token;
    }
    assert!(
        token.is_empty(),
        "paging did not terminate within 50 pages of size 1"
    );

    let whole = t.list_work_items(&tr, &q, 100, "").await.unwrap();
    assert_eq!(
        whole.items.len(),
        seen.len(),
        "paging must visit exactly the items a single large page returns"
    );
}

/// `get_work_item` returns the FULL description.
///
/// A list RPC may leave `description` empty when full bodies are too expensive;
/// `get` may not. The item body is the agent's brief, and a silently clipped
/// brief produces a confidently wrong change rather than a visible failure.
pub async fn get_returns_the_full_description(fx: &dyn Fixture) {
    let t = fx.tracker();
    let item = t.get_work_item(&fx.scratch_item()).await.unwrap();
    assert!(
        item.r#ref.is_some(),
        "a fetched item must carry its own reference"
    );
    let r = item.r#ref.unwrap();
    assert!(
        !r.key.is_empty(),
        "adapters MUST populate the human key — it is what lands in branch names"
    );
    assert!(
        !item.description.is_empty(),
        "get_work_item must return the full body, never a clipped one"
    );
}

/// An item is resolvable by its human key alone.
///
/// A campaign persists `DEV-18395` on a CRD and comes back later — possibly from
/// a different process — with no provider UUID in hand.
pub async fn item_resolves_by_human_key(fx: &dyn Fixture) {
    let t = fx.tracker();
    let full = t.get_work_item(&fx.scratch_item()).await.unwrap();
    let r = full.r#ref.clone().unwrap();

    let by_key = WorkItemRef {
        tracker: r.tracker.clone(),
        project: r.project.clone(),
        id: String::new(),
        key: r.key.clone(),
    };
    let again = t.get_work_item(&by_key).await.unwrap();
    assert_eq!(
        again.r#ref.unwrap().id,
        r.id,
        "a key-only reference must resolve to the same item"
    );
}

/// A surface an adapter declares false FAILS rather than returning a plausible
/// default.
///
/// The Linear adapter has no free-text search, and the honest answer to a text
/// query is an error — returning the unfiltered backlog instead would look like
/// a successful search that matched everything.
pub async fn unsupported_is_explicit(fx: &dyn Fixture) {
    let t = fx.tracker();
    let caps = t.capabilities().await.unwrap();
    let tr = fx.tracker_ref();

    if !caps.text_search {
        let q = WorkItemQuery {
            projects: vec![fx.project()],
            text: "some free text".to_string(),
            ..Default::default()
        };
        assert!(
            t.list_work_items(&tr, &q, 10, "").await.is_err(),
            "an adapter declaring text_search=false MUST reject a text query, \
             not silently return unfiltered results"
        );
    }
    if !caps.transitions {
        assert!(
            t.transition(
                &fx.scratch_item(),
                &TransitionTarget::Category(StateCategory::Done)
            )
            .await
            .is_err(),
            "an adapter declaring transitions=false MUST reject a transition"
        );
    }
    if !caps.link_changes {
        assert!(
            t.link_change(&fx.scratch_item(), &ChangeLink::default())
                .await
                .is_err(),
            "an adapter declaring link_changes=false MUST reject a link"
        );
    }
}

/// Every surface an adapter declares TRUE really answers.
///
/// The mirror of [`unsupported_is_explicit`]: a capability set that over-claims
/// is worse than one that under-claims, because a caller gates on it.
pub async fn declared_capabilities_are_served(fx: &dyn Fixture) {
    let t = fx.tracker();
    let caps = t.capabilities().await.unwrap();
    let tr = fx.tracker_ref();

    if caps.projects {
        let projects = t.list_projects(&tr).await.unwrap();
        assert!(
            projects.iter().any(|p| p.key == fx.project()),
            "projects=true must list the fixture's own project"
        );
    }
    if caps.git_branch_names {
        let item = t.get_work_item(&fx.scratch_item()).await.unwrap();
        assert!(
            !crate::branch_name_for(&item).is_empty(),
            "git_branch_names=true must yield a usable branch name"
        );
    }
    let states = t.list_states(&tr, &fx.project()).await.unwrap();
    assert!(
        !states.is_empty(),
        "a project's workflow must be readable — a category target cannot \
         resolve without it"
    );
    assert!(
        states
            .iter()
            .any(|s| s.category == StateCategory::InProgress as i32),
        "the fixture's project must expose an InProgress state"
    );
}

/// Generate the whole conformance suite for one adapter fixture.
///
/// `tracker::conformance_suite!(my_adapter, MyFixture::new());` — that single
/// invocation is the entire integration.
#[macro_export]
macro_rules! conformance_suite {
    ($modname:ident, $fixture:expr) => {
        mod $modname {
            use super::*;

            macro_rules! case {
                ($name:ident) => {
                    #[tokio::test]
                    async fn $name() {
                        let fx = $fixture;
                        $crate::conformance::$name(&fx).await;
                    }
                };
            }

            case!(transition_is_idempotent);
            case!(category_target_resolves_and_is_reported);
            case!(comment_is_idempotent_under_a_key);
            case!(comment_without_a_key_is_not_deduped);
            case!(link_change_is_idempotent_by_url);
            case!(query_filters_narrow_and_never_over_return);
            case!(listing_pages_and_terminates);
            case!(get_returns_the_full_description);
            case!(item_resolves_by_human_key);
            case!(unsupported_is_explicit);
            case!(declared_capabilities_are_served);
        }
    };
}
