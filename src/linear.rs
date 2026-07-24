//! The Linear adapter — [`Tracker`] over Linear's GraphQL API.
//!
//! Everything Linear-specific in this crate lives here: the GraphQL documents,
//! the workflow-state vocabulary (`backlog`/`unstarted`/`started`/`completed`/
//! `canceled`/`triage`), and the numeric priority scale. Nothing above this
//! module knows Linear exists.
//!
//! # Credentials
//!
//! The adapter is constructed WITH the caller's token and holds it only for its
//! own lifetime; [`crate::gateway`] builds one per request. Linear accepts two
//! credential shapes and they are NOT interchangeable on the wire:
//!
//! * a personal API key (`lin_api_…`) goes in `Authorization` verbatim, with no
//!   scheme;
//! * an OAuth access token goes in as `Bearer <token>`.
//!
//! Sending a personal key as `Bearer` is a 401 with a message that says nothing
//! about the scheme, so [`LinearTracker::auth_header`] picks by prefix rather
//! than making every caller remember.
//!
//! # What this adapter refuses
//!
//! `WorkItemQuery.text` is not supported: Linear's `issues` connection has no
//! free-text filter, and the search connection is a different resource with
//! different semantics. Per the contract, an adapter that cannot apply a set
//! filter FAILS rather than returning a superset — silently dropping `text`
//! would turn a narrow campaign into fan-out over the whole backlog. It is
//! declared `false` in [`Tracker::capabilities`] so a caller can check first.

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{
    ChangeLink, LinkedChange, PostedComment, Priority, Project, StateCategory, Tracker,
    TrackerCapabilities, TrackerError, TrackerKind, TrackerRef, TrackerResult, TransitionTarget,
    Transitioned, WorkItem, WorkItemPage, WorkItemQuery, WorkItemRef, WorkItemState,
};

/// Linear's public GraphQL endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://api.linear.app/graphql";

/// Marker appended to a comment body to carry an idempotency key.
///
/// An HTML comment, so it is invisible in Linear's rendered markdown while
/// staying greppable in the raw body we read back.
const IDEMPOTENCY_MARKER: &str = "fastverk-idempotency";

/// The GraphQL selection for one issue. Kept in one constant because it is
/// interpolated into three documents — list, get-by-id, get-by-key — and a
/// selection that drifts between them yields a `WorkItem` whose fields depend on
/// which RPC produced it.
const ISSUE_FIELDS: &str = r#"
  id
  identifier
  title
  description
  url
  createdAt
  updatedAt
  branchName
  estimate
  priority
  state { id name type }
  labels { nodes { name } }
  assignee { displayName }
  creator { displayName }
  team { key }
  parent { id identifier team { key } }
  project { name }
  cycle { name }
  attachments { nodes { url } }
"#;

/// [`Tracker`] over Linear.
pub struct LinearTracker {
    token: String,
    endpoint: String,
    http: reqwest::Client,
}

impl LinearTracker {
    /// An adapter authenticating as `token` against the public endpoint.
    pub fn new(token: impl Into<String>) -> Self {
        Self::with_endpoint(token, DEFAULT_ENDPOINT)
    }

    /// An adapter against a specific endpoint — for tests against a local
    /// GraphQL double.
    pub fn with_endpoint(token: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            endpoint: endpoint.into(),
            http: reqwest::Client::new(),
        }
    }

    /// The `Authorization` value for this token. Personal API keys are sent
    /// bare; anything else is treated as an OAuth token and gets `Bearer`.
    fn auth_header(&self) -> String {
        if self.token.starts_with("lin_api_") {
            self.token.clone()
        } else {
            format!("Bearer {}", self.token)
        }
    }

    /// Execute one GraphQL document and return its `data` object.
    ///
    /// Linear answers `200 OK` with a populated `errors` array for most
    /// application-level failures, so checking the HTTP status alone reports
    /// success on a failed query. Both paths are checked here.
    async fn graphql(&self, query: &str, variables: Value) -> TrackerResult<Value> {
        let resp = self
            .http
            .post(&self.endpoint)
            .header("Authorization", self.auth_header())
            .header("Content-Type", "application/json")
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .map_err(|e| TrackerError::msg(format!("linear request failed: {e}")))?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| TrackerError::msg(format!("linear returned non-JSON ({status}): {e}")))?;

        if let Some(errors) = body.get("errors").and_then(Value::as_array) {
            if !errors.is_empty() {
                let joined = errors
                    .iter()
                    .filter_map(|e| e.get("message").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(TrackerError::msg(format!("linear GraphQL error: {joined}")));
            }
        }
        if !status.is_success() {
            return Err(TrackerError::msg(format!("linear HTTP {status}")));
        }

        body.get("data")
            .cloned()
            .ok_or_else(|| TrackerError::msg("linear response had no data"))
    }

    /// Resolve a reference to Linear's own issue UUID.
    ///
    /// A campaign that persisted only the human key ("DEV-18395") still has to
    /// be able to come back and transition the item, so the key is a first-class
    /// lookup path rather than a display field.
    async fn resolve_id(&self, item: &WorkItemRef) -> TrackerResult<String> {
        if !item.id.is_empty() {
            return Ok(item.id.clone());
        }
        let issue = self.fetch_by_key(&item.key).await?;
        issue
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| TrackerError::msg(format!("no issue for key {}", item.key)))
    }

    /// Fetch one issue by its human key ("DEV-18395").
    async fn fetch_by_key(&self, key: &str) -> TrackerResult<Value> {
        let (team, number) = split_key(key)?;
        let query = format!(
            r#"query($team: String!, $number: Float!) {{
                 issues(filter: {{ team: {{ key: {{ eq: $team }} }}, number: {{ eq: $number }} }}, first: 1) {{
                   nodes {{ {ISSUE_FIELDS} }}
                 }}
               }}"#
        );
        let data = self
            .graphql(&query, json!({ "team": team, "number": number }))
            .await?;
        data.pointer("/issues/nodes/0")
            .cloned()
            .ok_or_else(|| TrackerError::msg(format!("no issue for key {key}")))
    }

    /// The workflow states of one team, by team key.
    async fn team_states(&self, team_key: &str) -> TrackerResult<Vec<Value>> {
        let query = r#"query($team: String!) {
              workflowStates(filter: { team: { key: { eq: $team } } }, first: 100) {
                nodes { id name type position }
              }
            }"#;
        let data = self.graphql(query, json!({ "team": team_key })).await?;
        Ok(data
            .pointer("/workflowStates/nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }
}

#[async_trait]
impl Tracker for LinearTracker {
    fn kind(&self) -> TrackerKind {
        TrackerKind::Linear
    }

    async fn list_work_items(
        &self,
        tracker: &TrackerRef,
        query: &WorkItemQuery,
        page_size: i32,
        page_token: &str,
    ) -> TrackerResult<WorkItemPage> {
        // Refuse rather than over-return. See the module docs.
        if !query.text.is_empty() {
            return Err(TrackerError::msg(
                "linear adapter does not support free-text query (capabilities.text_search=false)",
            ));
        }

        let filter = build_filter(query)?;
        let first = if page_size > 0 { page_size } else { 50 };
        let document = format!(
            r#"query($filter: IssueFilter, $first: Int!, $after: String) {{
                 issues(filter: $filter, first: $first, after: $after, orderBy: updatedAt) {{
                   nodes {{ {ISSUE_FIELDS} }}
                   pageInfo {{ hasNextPage endCursor }}
                 }}
               }}"#
        );
        let vars = json!({
            "filter": filter,
            "first": first,
            "after": if page_token.is_empty() { Value::Null } else { json!(page_token) },
        });

        let data = self.graphql(&document, vars).await?;
        let nodes = data
            .pointer("/issues/nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let items = nodes.iter().map(|n| issue_to_work_item(n, tracker)).collect();

        let has_next = data
            .pointer("/issues/pageInfo/hasNextPage")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_page_token = if has_next {
            data.pointer("/issues/pageInfo/endCursor")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        } else {
            String::new()
        };

        Ok(WorkItemPage {
            items,
            next_page_token,
        })
    }

    async fn get_work_item(&self, item: &WorkItemRef) -> TrackerResult<WorkItem> {
        let tracker = item.tracker.clone().unwrap_or_default();
        let node = if item.id.is_empty() {
            self.fetch_by_key(&item.key).await?
        } else {
            let document = format!(
                r#"query($id: String!) {{ issue(id: $id) {{ {ISSUE_FIELDS} }} }}"#
            );
            self.graphql(&document, json!({ "id": item.id }))
                .await?
                .get("issue")
                .cloned()
                .filter(|v| !v.is_null())
                .ok_or_else(|| TrackerError::msg(format!("no issue {}", item.id)))?
        };
        Ok(issue_to_work_item(&node, &tracker))
    }

    async fn list_projects(&self, tracker: &TrackerRef) -> TrackerResult<Vec<Project>> {
        // A Linear TEAM is what `WorkItemRef.project` keys on — it is the level
        // that owns a workflow and issue numbering. Linear also has a resource
        // literally called "project", which is a grouping WITHIN a team; that
        // one surfaces as `WorkItem.group`.
        let query = r#"query { teams(first: 250) { nodes { id key name } } }"#;
        let data = self.graphql(query, json!({})).await?;
        let nodes = data
            .pointer("/teams/nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(nodes
            .iter()
            .map(|n| Project {
                tracker: Some(tracker.clone()),
                key: str_at(n, "key"),
                name: str_at(n, "name"),
                url: String::new(),
            })
            .collect())
    }

    async fn list_states(
        &self,
        _tracker: &TrackerRef,
        project: &str,
    ) -> TrackerResult<Vec<WorkItemState>> {
        let nodes = if project.is_empty() {
            let query = r#"query { workflowStates(first: 250) { nodes { id name type } } }"#;
            self.graphql(query, json!({}))
                .await?
                .pointer("/workflowStates/nodes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        } else {
            self.team_states(project).await?
        };
        Ok(nodes.iter().map(state_from_node).collect())
    }

    async fn transition(
        &self,
        item: &WorkItemRef,
        target: &TransitionTarget,
    ) -> TrackerResult<Transitioned> {
        let current = self.get_work_item(item).await?;
        let current_state = current.state.clone().unwrap_or_default();
        let id = self.resolve_id(item).await?;

        let desired = match target {
            TransitionTarget::StateId(sid) => sid.clone(),
            TransitionTarget::Category(cat) => {
                // Resolve against the item's OWN team workflow — Linear defines
                // states per team, so a state id from another team is a valid
                // id that silently belongs to the wrong board.
                let team = current
                    .r#ref
                    .as_ref()
                    .map(|r| r.project.clone())
                    .unwrap_or_default();
                if team.is_empty() {
                    return Err(TrackerError::msg(
                        "cannot resolve a state category without the item's team",
                    ));
                }
                let states = self.team_states(&team).await?;
                let mut matching: Vec<&Value> = states
                    .iter()
                    .filter(|n| category_of(&str_at(n, "type")) == *cat)
                    .collect();
                // Deterministic pick: the workflow's own ordering, so the same
                // category always resolves to the same state.
                matching.sort_by(|a, b| {
                    let pa = a.get("position").and_then(Value::as_f64).unwrap_or(0.0);
                    let pb = b.get("position").and_then(Value::as_f64).unwrap_or(0.0);
                    pa.partial_cmp(&pb).unwrap_or(std::cmp::Ordering::Equal)
                });
                match matching.first() {
                    Some(n) => str_at(n, "id"),
                    None => {
                        return Err(TrackerError::msg(format!(
                            "team {team} has no workflow state in category {cat:?}"
                        )))
                    }
                }
            }
        };

        if current_state.id == desired {
            return Ok(Transitioned {
                state: current_state,
                changed: false,
            });
        }

        let mutation = r#"mutation($id: String!, $stateId: String!) {
              issueUpdate(id: $id, input: { stateId: $stateId }) {
                success
                issue { state { id name type } }
              }
            }"#;
        let data = self
            .graphql(mutation, json!({ "id": id, "stateId": desired }))
            .await?;
        if !data
            .pointer("/issueUpdate/success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(TrackerError::msg("linear issueUpdate reported failure"));
        }
        let state = data
            .pointer("/issueUpdate/issue/state")
            .map(state_from_node)
            .unwrap_or_default();
        Ok(Transitioned {
            state,
            changed: true,
        })
    }

    async fn comment(
        &self,
        item: &WorkItemRef,
        body: &str,
        idempotency_key: &str,
    ) -> TrackerResult<PostedComment> {
        let id = self.resolve_id(item).await?;

        if !idempotency_key.is_empty() {
            let query = r#"query($id: String!) {
                  issue(id: $id) { comments(first: 250) { nodes { id url body } } }
                }"#;
            let data = self.graphql(query, json!({ "id": id })).await?;
            let marker = marker_for(idempotency_key);
            if let Some(existing) = data
                .pointer("/issue/comments/nodes")
                .and_then(Value::as_array)
                .and_then(|nodes| {
                    nodes
                        .iter()
                        .find(|n| str_at(n, "body").contains(&marker))
                        .cloned()
                })
            {
                return Ok(PostedComment {
                    id: str_at(&existing, "id"),
                    url: str_at(&existing, "url"),
                    created: false,
                });
            }
        }

        let full_body = if idempotency_key.is_empty() {
            body.to_string()
        } else {
            format!("{body}\n\n{}", marker_for(idempotency_key))
        };
        let mutation = r#"mutation($issueId: String!, $body: String!) {
              commentCreate(input: { issueId: $issueId, body: $body }) {
                success
                comment { id url }
              }
            }"#;
        let data = self
            .graphql(mutation, json!({ "issueId": id, "body": full_body }))
            .await?;
        if !data
            .pointer("/commentCreate/success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(TrackerError::msg("linear commentCreate reported failure"));
        }
        Ok(PostedComment {
            id: data
                .pointer("/commentCreate/comment/id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            url: data
                .pointer("/commentCreate/comment/url")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            created: true,
        })
    }

    async fn link_change(
        &self,
        item: &WorkItemRef,
        link: &ChangeLink,
    ) -> TrackerResult<LinkedChange> {
        if link.url.is_empty() {
            return Err(TrackerError::msg("link_change requires a url"));
        }
        let id = self.resolve_id(item).await?;

        // Idempotent by URL: check before creating rather than relying on the
        // provider to dedupe.
        let query = r#"query($id: String!) {
              issue(id: $id) { attachments(first: 250) { nodes { id url } } }
            }"#;
        let data = self.graphql(query, json!({ "id": id })).await?;
        if let Some(existing) = data
            .pointer("/issue/attachments/nodes")
            .and_then(Value::as_array)
            .and_then(|nodes| {
                nodes
                    .iter()
                    .find(|n| str_at(n, "url") == link.url)
                    .cloned()
            })
        {
            return Ok(LinkedChange {
                id: str_at(&existing, "id"),
                created: false,
            });
        }

        let title = if link.title.is_empty() {
            if link.change_ref.is_empty() {
                link.url.clone()
            } else {
                link.change_ref.clone()
            }
        } else {
            link.title.clone()
        };
        let mutation = r#"mutation($issueId: String!, $url: String!, $title: String!) {
              attachmentLinkURL(issueId: $issueId, url: $url, title: $title) {
                success
                attachment { id }
              }
            }"#;
        let data = self
            .graphql(
                mutation,
                json!({ "issueId": id, "url": link.url, "title": title }),
            )
            .await?;
        if !data
            .pointer("/attachmentLinkURL/success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(TrackerError::msg("linear attachmentLinkURL reported failure"));
        }
        Ok(LinkedChange {
            id: data
                .pointer("/attachmentLinkURL/attachment/id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            created: true,
        })
    }

    async fn capabilities(&self) -> TrackerResult<TrackerCapabilities> {
        Ok(TrackerCapabilities {
            transitions: true,
            comments: true,
            link_changes: true,
            projects: true,
            estimates: true,
            sub_items: true,
            git_branch_names: true,
            // Deliberately false — see the module docs.
            text_search: false,
            labels: true,
        })
    }
}

// ============================================================
// Mapping
// ============================================================

/// Linear's `WorkflowState.type` vocabulary → the normalized category.
///
/// `triage` maps to BACKLOG: an untriaged item is explicitly not yet committed
/// work, which is what BACKLOG means here. Mapping it to TODO would sweep
/// untriaged items into a fan-out whose query asked for ready work.
fn category_of(state_type: &str) -> StateCategory {
    match state_type {
        "backlog" | "triage" => StateCategory::Backlog,
        "unstarted" => StateCategory::Todo,
        "started" => StateCategory::InProgress,
        "completed" => StateCategory::Done,
        "canceled" => StateCategory::Canceled,
        _ => StateCategory::Unspecified,
    }
}

/// The Linear `WorkflowState.type` values in a normalized category.
fn types_in(cat: StateCategory) -> &'static [&'static str] {
    match cat {
        StateCategory::Backlog => &["backlog", "triage"],
        StateCategory::Todo => &["unstarted"],
        StateCategory::InProgress => &["started"],
        StateCategory::Done => &["completed"],
        StateCategory::Canceled => &["canceled"],
        StateCategory::Unspecified => &[],
    }
}

fn state_from_node(n: &Value) -> WorkItemState {
    WorkItemState {
        id: str_at(n, "id"),
        name: str_at(n, "name"),
        category: category_of(&str_at(n, "type")) as i32,
    }
}

/// Linear's numeric priority is the contract's scale exactly: 0 none, 1 urgent,
/// 2 high, 3 medium, 4 low. Anything else is UNSPECIFIED rather than a guess.
fn priority_from(n: i64) -> Priority {
    match n {
        1 => Priority::Urgent,
        2 => Priority::High,
        3 => Priority::Medium,
        4 => Priority::Low,
        _ => Priority::Unspecified,
    }
}

fn str_at(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn nested_str(v: &Value, path: &str) -> String {
    v.pointer(path)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// One Linear issue node → the contract's [`WorkItem`].
fn issue_to_work_item(n: &Value, tracker: &TrackerRef) -> WorkItem {
    let team_key = nested_str(n, "/team/key");
    let item_ref = WorkItemRef {
        tracker: Some(tracker.clone()),
        project: team_key,
        id: str_at(n, "id"),
        key: str_at(n, "identifier"),
    };

    let labels = n
        .pointer("/labels/nodes")
        .and_then(Value::as_array)
        .map(|nodes| nodes.iter().map(|l| str_at(l, "name")).collect())
        .unwrap_or_default();

    let change_refs = n
        .pointer("/attachments/nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .map(|a| str_at(a, "url"))
                .filter(|u| !u.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let parent = n.get("parent").filter(|p| !p.is_null()).map(|p| WorkItemRef {
        tracker: Some(tracker.clone()),
        project: nested_str(p, "/team/key"),
        id: str_at(p, "id"),
        key: str_at(p, "identifier"),
    });

    WorkItem {
        r#ref: Some(item_ref),
        title: str_at(n, "title"),
        description: str_at(n, "description"),
        state: n.get("state").map(state_from_node),
        priority: priority_from(n.get("priority").and_then(Value::as_i64).unwrap_or(0)) as i32,
        labels,
        assignee: nested_str(n, "/assignee/displayName"),
        author: nested_str(n, "/creator/displayName"),
        estimate: n.get("estimate").and_then(Value::as_f64).unwrap_or(0.0),
        url: str_at(n, "url"),
        created_at: str_at(n, "createdAt"),
        updated_at: str_at(n, "updatedAt"),
        git_branch_name: str_at(n, "branchName"),
        parent,
        change_refs,
        group: nested_str(n, "/project/name"),
        cycle: nested_str(n, "/cycle/name"),
    }
}

/// "DEV-18395" → ("DEV", 18395).
fn split_key(key: &str) -> TrackerResult<(String, f64)> {
    let (team, num) = key
        .rsplit_once('-')
        .ok_or_else(|| TrackerError::msg(format!("not a linear issue key: {key}")))?;
    let number: f64 = num
        .parse()
        .map_err(|_| TrackerError::msg(format!("not a linear issue key: {key}")))?;
    Ok((team.to_string(), number))
}

fn marker_for(key: &str) -> String {
    format!("<!-- {IDEMPOTENCY_MARKER}: {key} -->")
}

/// The contract's [`WorkItemQuery`] → a Linear `IssueFilter`.
///
/// Every clause is ANDed. Label matching is the subtle one: the contract says
/// the item must carry ALL the listed labels, and Linear's `labels: { every: … }`
/// means "every label ON THE ISSUE matches", which is the opposite question — so
/// each required label becomes its own `some` clause under `and`.
fn build_filter(q: &WorkItemQuery) -> TrackerResult<Value> {
    let mut and: Vec<Value> = Vec::new();

    if !q.projects.is_empty() {
        and.push(json!({ "team": { "key": { "in": q.projects } } }));
    }

    if !q.state_categories.is_empty() {
        let mut types: Vec<&str> = Vec::new();
        for c in &q.state_categories {
            let cat = StateCategory::try_from(*c).unwrap_or(StateCategory::Unspecified);
            if cat == StateCategory::Unspecified {
                return Err(TrackerError::msg(
                    "STATE_CATEGORY_UNSPECIFIED is not a filter value",
                ));
            }
            types.extend_from_slice(types_in(cat));
        }
        and.push(json!({ "state": { "type": { "in": types } } }));
    }

    for label in &q.labels {
        and.push(json!({ "labels": { "some": { "name": { "eq": label } } } }));
    }

    if !q.assignees.is_empty() {
        and.push(json!({ "assignee": { "displayName": { "in": q.assignees } } }));
    }

    if !q.updated_since.is_empty() {
        and.push(json!({ "updatedAt": { "gte": q.updated_since } }));
    }

    let min = Priority::try_from(q.min_priority).unwrap_or(Priority::Unspecified);
    if min != Priority::Unspecified {
        // Linear's scale runs most-urgent-first (1 = Urgent), and 0 means "no
        // priority" rather than "lowest" — so a minimum is `<= n AND >= 1`, and
        // dropping the lower bound would sweep in every unprioritized item.
        and.push(json!({ "priority": { "lte": min as i32, "gte": 1 } }));
    }

    Ok(if and.is_empty() {
        json!({})
    } else {
        json!({ "and": and })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_scheme_is_picked_by_token_prefix() {
        assert_eq!(
            LinearTracker::new("lin_api_abc").auth_header(),
            "lin_api_abc"
        );
        assert_eq!(
            LinearTracker::new("oauth-token").auth_header(),
            "Bearer oauth-token"
        );
    }

    #[test]
    fn key_splits_into_team_and_number() {
        assert_eq!(split_key("DEV-18395").unwrap(), ("DEV".to_string(), 18395.0));
        assert!(split_key("nonsense").is_err());
        assert!(split_key("DEV-x").is_err());
    }

    #[test]
    fn triage_is_backlog_not_todo() {
        assert_eq!(category_of("triage"), StateCategory::Backlog);
        assert_eq!(category_of("unstarted"), StateCategory::Todo);
        assert_eq!(category_of("started"), StateCategory::InProgress);
        assert_eq!(category_of("completed"), StateCategory::Done);
        assert_eq!(category_of("canceled"), StateCategory::Canceled);
        assert_eq!(category_of("invented"), StateCategory::Unspecified);
    }

    #[test]
    fn every_required_label_becomes_its_own_some_clause() {
        let q = WorkItemQuery {
            labels: vec!["agent/ready".into(), "bug".into()],
            ..Default::default()
        };
        let f = build_filter(&q).unwrap();
        let and = f["and"].as_array().unwrap();
        assert_eq!(and.len(), 2);
        assert_eq!(and[0]["labels"]["some"]["name"]["eq"], "agent/ready");
        assert_eq!(and[1]["labels"]["some"]["name"]["eq"], "bug");
    }

    #[test]
    fn state_categories_expand_to_linear_types() {
        let q = WorkItemQuery {
            state_categories: vec![StateCategory::Todo as i32, StateCategory::Backlog as i32],
            ..Default::default()
        };
        let f = build_filter(&q).unwrap();
        let types = f["and"][0]["state"]["type"]["in"].as_array().unwrap();
        let types: Vec<&str> = types.iter().map(|t| t.as_str().unwrap()).collect();
        assert_eq!(types, vec!["unstarted", "backlog", "triage"]);
    }

    #[test]
    fn min_priority_keeps_the_lower_bound_so_unprioritized_is_excluded() {
        let q = WorkItemQuery {
            min_priority: Priority::High as i32,
            ..Default::default()
        };
        let f = build_filter(&q).unwrap();
        assert_eq!(f["and"][0]["priority"]["lte"], 2);
        assert_eq!(f["and"][0]["priority"]["gte"], 1);
    }

    #[test]
    fn empty_query_is_an_empty_filter_not_a_malformed_one() {
        let f = build_filter(&WorkItemQuery::default()).unwrap();
        assert_eq!(f, json!({}));
    }

    #[test]
    fn unspecified_category_is_rejected_rather_than_silently_dropped() {
        let q = WorkItemQuery {
            state_categories: vec![StateCategory::Unspecified as i32],
            ..Default::default()
        };
        assert!(build_filter(&q).is_err());
    }
}
