//! In-memory [`Tracker`] double.
//!
//! A real module behind the non-default `testing` feature rather than
//! `#[cfg(test)]`, because an adapter living in its own Bazel module cannot see
//! this crate's test-only code — and an adapter that cannot exercise the shared
//! double ends up copying it, which is how a contract quietly forks.
//!
//! [`FakeTracker`] implements the contract's *semantics*, not just its shape:
//! transitions are idempotent, comments dedupe on an idempotency key, links
//! dedupe on URL, and every declared query filter is really applied. That is
//! what makes it usable as the reference the conformance suite runs first — a
//! double that ignored filters would let a suite pass while proving nothing.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::{
    ChangeLink, LinkedChange, PostedComment, Priority, Project, StateCategory, Tracker,
    TrackerCapabilities, TrackerError, TrackerKind, TrackerRef, TrackerResult, TransitionTarget,
    Transitioned, WorkItem, WorkItemPage, WorkItemQuery, WorkItemRef, WorkItemState,
};

/// A comment as recorded by the double.
#[derive(Debug, Clone)]
pub struct FakeComment {
    pub id: String,
    pub item_key: String,
    pub body: String,
    pub idempotency_key: String,
}

/// A change link as recorded by the double.
#[derive(Debug, Clone)]
pub struct FakeLink {
    pub id: String,
    pub item_key: String,
    pub url: String,
    pub title: String,
}

#[derive(Default)]
struct State {
    items: Vec<WorkItem>,
    /// Workflow states per project key.
    states: HashMap<String, Vec<WorkItemState>>,
    comments: Vec<FakeComment>,
    links: Vec<FakeLink>,
    seq: u64,
}

/// An in-memory tracker.
pub struct FakeTracker {
    inner: Mutex<State>,
    kind: TrackerKind,
    capabilities: TrackerCapabilities,
}

impl Default for FakeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeTracker {
    /// An empty tracker declaring the full contract.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(State::default()),
            kind: TrackerKind::Unspecified,
            capabilities: TrackerCapabilities {
                transitions: true,
                comments: true,
                link_changes: true,
                projects: true,
                estimates: true,
                sub_items: true,
                git_branch_names: true,
                text_search: true,
                labels: true,
            },
        }
    }

    /// Declare a narrower capability set — for asserting that a caller which
    /// checks capabilities first really does skip the surface.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: TrackerCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Seed the default workflow for `project`: Backlog → Todo → In Progress →
    /// Done → Canceled, in that order.
    pub fn seed_default_states(&self, project: &str) {
        let states = vec![
            state("s-backlog", "Backlog", StateCategory::Backlog),
            state("s-todo", "Todo", StateCategory::Todo),
            state("s-doing", "In Progress", StateCategory::InProgress),
            state("s-done", "Done", StateCategory::Done),
            state("s-canceled", "Canceled", StateCategory::Canceled),
        ];
        self.inner
            .lock()
            .expect("fake tracker state")
            .states
            .insert(project.to_string(), states);
    }

    /// Seed one item.
    pub fn seed_item(&self, item: WorkItem) {
        self.inner
            .lock()
            .expect("fake tracker state")
            .items
            .push(item);
    }

    /// Every comment recorded, in post order.
    #[must_use]
    pub fn comments(&self) -> Vec<FakeComment> {
        self.inner.lock().expect("fake tracker state").comments.clone()
    }

    /// Every link recorded, in creation order.
    #[must_use]
    pub fn links(&self) -> Vec<FakeLink> {
        self.inner.lock().expect("fake tracker state").links.clone()
    }

    fn next_id(&self, prefix: &str) -> String {
        let mut g = self.inner.lock().expect("fake tracker state");
        g.seq += 1;
        format!("{prefix}-{}", g.seq)
    }
}

/// A [`WorkItemState`] literal.
#[must_use]
pub fn state(id: &str, name: &str, category: StateCategory) -> WorkItemState {
    WorkItemState {
        id: id.to_string(),
        name: name.to_string(),
        category: category as i32,
    }
}

/// A minimal [`WorkItem`] in `project` with key `key`, in the Todo state.
#[must_use]
pub fn item(project: &str, key: &str, title: &str) -> WorkItem {
    WorkItem {
        r#ref: Some(WorkItemRef {
            tracker: None,
            project: project.to_string(),
            id: format!("id-{key}"),
            key: key.to_string(),
        }),
        title: title.to_string(),
        description: format!("body of {key}"),
        state: Some(state("s-todo", "Todo", StateCategory::Todo)),
        priority: Priority::Medium as i32,
        git_branch_name: key.to_lowercase(),
        ..Default::default()
    }
}

fn matches(it: &WorkItem, q: &WorkItemQuery) -> bool {
    let r = it.r#ref.clone().unwrap_or_default();

    if !q.projects.is_empty() && !q.projects.contains(&r.project) {
        return false;
    }
    if !q.state_categories.is_empty() {
        let cat = it.state.as_ref().map(|s| s.category).unwrap_or(0);
        if !q.state_categories.contains(&cat) {
            return false;
        }
    }
    // ALL listed labels must be present — the contract's reading.
    if !q.labels.iter().all(|l| it.labels.contains(l)) {
        return false;
    }
    if !q.assignees.is_empty() && !q.assignees.contains(&it.assignee) {
        return false;
    }
    if !q.updated_since.is_empty() && it.updated_at.as_str() < q.updated_since.as_str() {
        return false;
    }
    if !q.text.is_empty() {
        let hay = format!("{} {}", it.title, it.description).to_lowercase();
        if !hay.contains(&q.text.to_lowercase()) {
            return false;
        }
    }
    if q.min_priority != Priority::Unspecified as i32 {
        // Most-urgent-first, and 0 ("none") is excluded rather than treated as
        // the lowest — the same reading the Linear adapter implements.
        if it.priority == Priority::Unspecified as i32 || it.priority > q.min_priority {
            return false;
        }
    }
    true
}

#[async_trait]
impl Tracker for FakeTracker {
    fn kind(&self) -> TrackerKind {
        self.kind
    }

    async fn list_work_items(
        &self,
        tracker: &TrackerRef,
        query: &WorkItemQuery,
        page_size: i32,
        page_token: &str,
    ) -> TrackerResult<WorkItemPage> {
        if !query.text.is_empty() && !self.capabilities.text_search {
            return Err(TrackerError::msg("text search unsupported by this adapter"));
        }

        let g = self.inner.lock().expect("fake tracker state");
        let all: Vec<WorkItem> = g
            .items
            .iter()
            .filter(|it| matches(it, query))
            .map(|it| {
                let mut it = it.clone();
                if let Some(r) = it.r#ref.as_mut() {
                    r.tracker = Some(tracker.clone());
                }
                it
            })
            .collect();

        let start: usize = if page_token.is_empty() {
            0
        } else {
            page_token
                .parse()
                .map_err(|_| TrackerError::msg("bad page token"))?
        };
        let size = if page_size > 0 { page_size as usize } else { 50 };
        let end = (start + size).min(all.len());
        let items = all.get(start..end).unwrap_or_default().to_vec();
        let next_page_token = if end < all.len() {
            end.to_string()
        } else {
            String::new()
        };

        Ok(WorkItemPage {
            items,
            next_page_token,
        })
    }

    async fn get_work_item(&self, item: &WorkItemRef) -> TrackerResult<WorkItem> {
        let g = self.inner.lock().expect("fake tracker state");
        g.items
            .iter()
            .find(|it| {
                let r = it.r#ref.clone().unwrap_or_default();
                (!item.id.is_empty() && r.id == item.id)
                    || (!item.key.is_empty() && r.key == item.key)
            })
            .cloned()
            .ok_or_else(|| TrackerError::msg(format!("no item {}", crate::item_slug(item))))
    }

    async fn list_projects(&self, tracker: &TrackerRef) -> TrackerResult<Vec<Project>> {
        let g = self.inner.lock().expect("fake tracker state");
        let mut keys: Vec<String> = g.states.keys().cloned().collect();
        keys.sort();
        Ok(keys
            .into_iter()
            .map(|key| Project {
                tracker: Some(tracker.clone()),
                name: key.clone(),
                key,
                url: String::new(),
            })
            .collect())
    }

    async fn list_states(
        &self,
        _tracker: &TrackerRef,
        project: &str,
    ) -> TrackerResult<Vec<WorkItemState>> {
        let g = self.inner.lock().expect("fake tracker state");
        if project.is_empty() {
            let mut all: Vec<WorkItemState> = g.states.values().flatten().cloned().collect();
            all.sort_by(|a, b| a.id.cmp(&b.id));
            return Ok(all);
        }
        Ok(g.states.get(project).cloned().unwrap_or_default())
    }

    async fn transition(
        &self,
        item: &WorkItemRef,
        target: &TransitionTarget,
    ) -> TrackerResult<Transitioned> {
        if !self.capabilities.transitions {
            return Err(TrackerError::msg("transitions unsupported by this adapter"));
        }
        let mut g = self.inner.lock().expect("fake tracker state");

        let idx = g
            .items
            .iter()
            .position(|it| {
                let r = it.r#ref.clone().unwrap_or_default();
                (!item.id.is_empty() && r.id == item.id)
                    || (!item.key.is_empty() && r.key == item.key)
            })
            .ok_or_else(|| TrackerError::msg(format!("no item {}", crate::item_slug(item))))?;

        let project = g.items[idx]
            .r#ref
            .clone()
            .unwrap_or_default()
            .project
            .clone();
        let workflow = g.states.get(&project).cloned().unwrap_or_default();

        let desired = match target {
            TransitionTarget::StateId(sid) => workflow
                .iter()
                .find(|s| &s.id == sid)
                .cloned()
                .ok_or_else(|| TrackerError::msg(format!("no state {sid} in {project}")))?,
            TransitionTarget::Category(cat) => workflow
                .iter()
                .find(|s| s.category == *cat as i32)
                .cloned()
                .ok_or_else(|| {
                    TrackerError::msg(format!("{project} has no state in category {cat:?}"))
                })?,
        };

        let current = g.items[idx].state.clone().unwrap_or_default();
        if current.id == desired.id {
            return Ok(Transitioned {
                state: current,
                changed: false,
            });
        }
        g.items[idx].state = Some(desired.clone());
        Ok(Transitioned {
            state: desired,
            changed: true,
        })
    }

    async fn comment(
        &self,
        item: &WorkItemRef,
        body: &str,
        idempotency_key: &str,
    ) -> TrackerResult<PostedComment> {
        if !self.capabilities.comments {
            return Err(TrackerError::msg("comments unsupported by this adapter"));
        }
        let slug = crate::item_slug(item);

        if !idempotency_key.is_empty() {
            let g = self.inner.lock().expect("fake tracker state");
            if let Some(existing) = g
                .comments
                .iter()
                .find(|c| c.item_key == slug && c.idempotency_key == idempotency_key)
            {
                return Ok(PostedComment {
                    id: existing.id.clone(),
                    url: format!("https://tracker.invalid/c/{}", existing.id),
                    created: false,
                });
            }
        }

        let id = self.next_id("comment");
        let mut g = self.inner.lock().expect("fake tracker state");
        g.comments.push(FakeComment {
            id: id.clone(),
            item_key: slug,
            body: body.to_string(),
            idempotency_key: idempotency_key.to_string(),
        });
        Ok(PostedComment {
            url: format!("https://tracker.invalid/c/{id}"),
            id,
            created: true,
        })
    }

    async fn link_change(
        &self,
        item: &WorkItemRef,
        link: &ChangeLink,
    ) -> TrackerResult<LinkedChange> {
        if !self.capabilities.link_changes {
            return Err(TrackerError::msg("change links unsupported by this adapter"));
        }
        if link.url.is_empty() {
            return Err(TrackerError::msg("link_change requires a url"));
        }
        let slug = crate::item_slug(item);

        {
            let g = self.inner.lock().expect("fake tracker state");
            if let Some(existing) = g
                .links
                .iter()
                .find(|l| l.item_key == slug && l.url == link.url)
            {
                return Ok(LinkedChange {
                    id: existing.id.clone(),
                    created: false,
                });
            }
        }

        let id = self.next_id("link");
        let mut g = self.inner.lock().expect("fake tracker state");
        g.links.push(FakeLink {
            id: id.clone(),
            item_key: slug,
            url: link.url.clone(),
            title: link.title.clone(),
        });
        Ok(LinkedChange { id, created: true })
    }

    async fn capabilities(&self) -> TrackerResult<TrackerCapabilities> {
        Ok(self.capabilities.clone())
    }
}
