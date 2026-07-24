//! Conformance run for the in-memory fake adapter.
//!
//! The suite itself lives in `tracker::conformance` (a library module behind the
//! `testing` feature) so out-of-crate adapters can run the identical battery.
//! This file only supplies a fixture and invokes it.

use tracker::conformance::Fixture;
use tracker::testing::{item, FakeTracker};
use tracker::{Priority, Tracker, TrackerKind, TrackerRef, WorkItemRef};

const PROJECT: &str = "DEV";

/// The in-memory fixture. Real-adapter fixtures (linear, and later jira /
/// forge-issues) plug in beside this one.
struct FakeFixture {
    tracker: FakeTracker,
    tracker_ref: TrackerRef,
    scratch: WorkItemRef,
}

impl FakeFixture {
    fn new(kind: TrackerKind) -> Self {
        let tracker_ref = TrackerRef {
            tracker: kind as i32,
            host: "tracker.invalid".to_string(),
            workspace: "acme".to_string(),
        };

        let tracker = FakeTracker::new();
        tracker.seed_default_states(PROJECT);

        // The item every mutating case works on.
        let mut scratch = item(PROJECT, "DEV-1", "the scratch item");
        scratch.updated_at = "2026-07-24T00:00:00Z".to_string();
        scratch.labels = vec!["agent/ready".to_string()];
        let scratch_ref = scratch.r#ref.clone().unwrap();
        tracker.seed_item(scratch);

        // A second item so paging has something to page over, and so a filter
        // that narrows has something to narrow AWAY.
        let mut other = item(PROJECT, "DEV-2", "another item");
        other.updated_at = "2026-07-23T00:00:00Z".to_string();
        other.priority = Priority::Low as i32;
        tracker.seed_item(other);

        Self {
            tracker,
            tracker_ref,
            scratch: scratch_ref,
        }
    }
}

impl Fixture for FakeFixture {
    fn tracker(&self) -> &dyn Tracker {
        &self.tracker
    }
    fn tracker_ref(&self) -> TrackerRef {
        self.tracker_ref.clone()
    }
    fn project(&self) -> String {
        PROJECT.to_string()
    }
    fn scratch_item(&self) -> WorkItemRef {
        self.scratch.clone()
    }
}

tracker::conformance_suite!(fake_linear, FakeFixture::new(TrackerKind::Linear));

// TODO(linear): a real-adapter fixture needs recorded HTTP fixtures so it runs
// offline in CI — record once against a scratch Linear workspace, replay
// thereafter. Until then the Linear adapter is covered by its unit tests (filter
// construction, state mapping, auth scheme) and the contract is pinned here.
