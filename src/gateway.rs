//! tracker-gateway — the gRPC daemon serving `tracker.v1.TrackerService`.
//!
//! The single-source-of-truth server for work-tracker operations: it implements
//! the generated `TrackerService` over the crate's [`Tracker`] trait,
//! dispatching each RPC to a per-request adapter. The daemon holds **no**
//! tracker credential of its own — the caller's identity travels in request
//! metadata (`x-fastverk-linear-token`), exactly as forge-gateway takes
//! `x-fastverk-gitlab-token` — so every op runs as the caller.
//!
//! Two consumers share this one implementation: the agents campaign controller
//! (which turns a backlog query into a `FanoutRun`) and `plugin-tracker`, whose
//! agent-callable MCP tools proxy to it — so "the agent can read Linear" and
//! "the controller can read Linear" are the same code path with different
//! credentials, not two integrations that drift.

use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::linear::LinearTracker;
use crate::pb::tracker_service_server::{TrackerService, TrackerServiceServer};
use crate::pb::{
    CommentOnWorkItemRequest, CommentOnWorkItemResponse, GetCapabilitiesRequest,
    GetCapabilitiesResponse, GetWorkItemRequest, GetWorkItemResponse, LinkChangeRequest,
    LinkChangeResponse, ListProjectsRequest, ListProjectsResponse, ListStatesRequest,
    ListStatesResponse, ListWorkItemsRequest, ListWorkItemsResponse, TransitionWorkItemRequest,
    TransitionWorkItemResponse,
};
use crate::{
    ChangeLink, StateCategory, Tracker, TrackerError, TrackerKind, TrackerRef, TransitionTarget,
    WorkItemQuery, WorkItemRef,
};

/// Metadata key carrying the caller's Linear credential — a personal API key
/// (`lin_api_…`) or an OAuth access token; the adapter picks the scheme.
pub const LINEAR_TOKEN_META: &str = "x-fastverk-linear-token";

/// The `tracker.v1.TrackerService` implementation.
#[derive(Default)]
pub struct TrackerGateway {}

impl TrackerGateway {
    /// Wrap the gateway in its tonic server, ready to `add_service`.
    #[must_use]
    pub fn into_server(self) -> TrackerServiceServer<Self> {
        TrackerServiceServer::new(self)
    }

    /// Build the per-request adapter for `tracker` from the caller's metadata.
    ///
    /// An unrecognized `Tracker` value is REJECTED rather than defaulted — the
    /// rule the proto states, and the one `forge.v1` learned the hard way when
    /// "not GitHub therefore GitLab" became a wrong-API write.
    fn adapter(&self, tracker: &TrackerRef, meta: &MetadataMap) -> Result<Box<dyn Tracker>, Status> {
        match TrackerKind::try_from(tracker.tracker).unwrap_or(TrackerKind::Unspecified) {
            TrackerKind::Linear => {
                let token = meta_str(meta, LINEAR_TOKEN_META)
                    .ok_or_else(|| Status::unauthenticated("missing linear token"))?;
                Ok(Box::new(LinearTracker::new(token)))
            }
            TrackerKind::Unspecified => Err(Status::invalid_argument(
                "tracker unspecified — set TrackerRef.tracker",
            )),
            other => Err(Status::unimplemented(format!(
                "no adapter for tracker {}",
                other.as_str_name()
            ))),
        }
    }

    /// The adapter for a request addressed by a work item, whose reference
    /// embeds its own instance.
    fn adapter_for_item(
        &self,
        item: &WorkItemRef,
        meta: &MetadataMap,
    ) -> Result<Box<dyn Tracker>, Status> {
        let tracker = item
            .tracker
            .clone()
            .ok_or_else(|| Status::invalid_argument("WorkItemRef.tracker is required"))?;
        self.adapter(&tracker, meta)
    }
}

#[tonic::async_trait]
impl TrackerService for TrackerGateway {
    async fn list_work_items(
        &self,
        request: Request<ListWorkItemsRequest>,
    ) -> Result<Response<ListWorkItemsResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let tracker = require_tracker(req.tracker)?;
        let adapter = self.adapter(&tracker, &meta)?;
        let query = req.query.unwrap_or_else(WorkItemQuery::default);

        let page = adapter
            .list_work_items(&tracker, &query, req.page_size, &req.page_token)
            .await
            .map_err(to_status)?;
        Ok(Response::new(ListWorkItemsResponse {
            items: page.items,
            next_page_token: page.next_page_token,
        }))
    }

    async fn get_work_item(
        &self,
        request: Request<GetWorkItemRequest>,
    ) -> Result<Response<GetWorkItemResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let item = require_item(req.item)?;
        let adapter = self.adapter_for_item(&item, &meta)?;
        let found = adapter.get_work_item(&item).await.map_err(to_status)?;
        Ok(Response::new(GetWorkItemResponse { item: Some(found) }))
    }

    async fn list_projects(
        &self,
        request: Request<ListProjectsRequest>,
    ) -> Result<Response<ListProjectsResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let tracker = require_tracker(req.tracker)?;
        let adapter = self.adapter(&tracker, &meta)?;
        let projects = adapter.list_projects(&tracker).await.map_err(to_status)?;
        Ok(Response::new(ListProjectsResponse { projects }))
    }

    async fn list_states(
        &self,
        request: Request<ListStatesRequest>,
    ) -> Result<Response<ListStatesResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let tracker = require_tracker(req.tracker)?;
        let adapter = self.adapter(&tracker, &meta)?;
        let states = adapter
            .list_states(&tracker, &req.project)
            .await
            .map_err(to_status)?;
        Ok(Response::new(ListStatesResponse { states }))
    }

    async fn transition_work_item(
        &self,
        request: Request<TransitionWorkItemRequest>,
    ) -> Result<Response<TransitionWorkItemResponse>, Status> {
        use crate::pb::transition_work_item_request::Target;

        let (meta, _, req) = request.into_parts();
        let item = require_item(req.item)?;
        let adapter = self.adapter_for_item(&item, &meta)?;

        let target = match req.target {
            Some(Target::StateId(id)) => TransitionTarget::StateId(id),
            Some(Target::Category(c)) => {
                let cat = StateCategory::try_from(c).unwrap_or(StateCategory::Unspecified);
                if cat == StateCategory::Unspecified {
                    return Err(Status::invalid_argument(
                        "STATE_CATEGORY_UNSPECIFIED is not a transition target",
                    ));
                }
                TransitionTarget::Category(cat)
            }
            None => {
                return Err(Status::invalid_argument(
                    "transition target is required: set state_id or category",
                ))
            }
        };

        let done = adapter.transition(&item, &target).await.map_err(to_status)?;
        Ok(Response::new(TransitionWorkItemResponse {
            state: Some(done.state),
            changed: done.changed,
        }))
    }

    async fn comment_on_work_item(
        &self,
        request: Request<CommentOnWorkItemRequest>,
    ) -> Result<Response<CommentOnWorkItemResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let item = require_item(req.item)?;
        let adapter = self.adapter_for_item(&item, &meta)?;
        let posted = adapter
            .comment(&item, &req.body, &req.idempotency_key)
            .await
            .map_err(to_status)?;
        Ok(Response::new(CommentOnWorkItemResponse {
            id: posted.id,
            url: posted.url,
            created: posted.created,
        }))
    }

    async fn link_change(
        &self,
        request: Request<LinkChangeRequest>,
    ) -> Result<Response<LinkChangeResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let item = require_item(req.item)?;
        let adapter = self.adapter_for_item(&item, &meta)?;
        let linked = adapter
            .link_change(
                &item,
                &ChangeLink {
                    url: req.url,
                    change_ref: req.change_ref,
                    title: req.title,
                },
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(LinkChangeResponse {
            id: linked.id,
            created: linked.created,
        }))
    }

    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        let (meta, _, req) = request.into_parts();
        let tracker = require_tracker(req.tracker)?;
        let adapter = self.adapter(&tracker, &meta)?;
        let capabilities = adapter.capabilities().await.map_err(to_status)?;
        Ok(Response::new(GetCapabilitiesResponse {
            capabilities: Some(capabilities),
        }))
    }
}

fn require_tracker(t: Option<TrackerRef>) -> Result<TrackerRef, Status> {
    t.ok_or_else(|| Status::invalid_argument("TrackerRef is required"))
}

fn require_item(i: Option<WorkItemRef>) -> Result<WorkItemRef, Status> {
    let item = i.ok_or_else(|| Status::invalid_argument("WorkItemRef is required"))?;
    if item.id.is_empty() && item.key.is_empty() {
        return Err(Status::invalid_argument(
            "WorkItemRef needs an id or a key",
        ));
    }
    Ok(item)
}

fn meta_str(meta: &MetadataMap, key: &str) -> Option<String> {
    meta.get(key)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// A trait error → a gRPC status.
///
/// Deliberately `internal` rather than a guessed code: the trait's error is a
/// message, not a classified failure, and inventing `not_found` from substring
/// matching would let a caller branch on a guess. Adapters that need a
/// distinguishable code should gain a typed variant first.
fn to_status(e: TrackerError) -> Status {
    Status::internal(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::TrackerRef as PbTrackerRef;

    fn meta_with(key: &'static str, value: &str) -> MetadataMap {
        let mut m = MetadataMap::new();
        m.insert(key, value.parse().unwrap());
        m
    }

    /// `Box<dyn Tracker>` is not `Debug`, so `unwrap_err` cannot be used on an
    /// adapter result.
    fn err_code(r: Result<Box<dyn Tracker>, Status>) -> tonic::Code {
        match r {
            Ok(_) => panic!("expected an error, got an adapter"),
            Err(e) => e.code(),
        }
    }

    #[test]
    fn linear_needs_a_token() {
        let gw = TrackerGateway::default();
        let tr = PbTrackerRef {
            tracker: TrackerKind::Linear as i32,
            ..Default::default()
        };
        assert_eq!(
            err_code(gw.adapter(&tr, &MetadataMap::new())),
            tonic::Code::Unauthenticated
        );

        assert!(gw
            .adapter(&tr, &meta_with(LINEAR_TOKEN_META, "lin_api_x"))
            .is_ok());
    }

    #[test]
    fn unspecified_and_unknown_trackers_are_rejected_not_defaulted() {
        let gw = TrackerGateway::default();

        let unspecified = PbTrackerRef::default();
        assert_eq!(
            err_code(gw.adapter(&unspecified, &MetadataMap::new())),
            tonic::Code::InvalidArgument
        );

        let jira = PbTrackerRef {
            tracker: TrackerKind::Jira as i32,
            ..Default::default()
        };
        assert_eq!(
            err_code(gw.adapter(&jira, &MetadataMap::new())),
            tonic::Code::Unimplemented,
            "an unimplemented tracker must be Unimplemented, never silently \
             routed to another adapter"
        );
    }

    #[test]
    fn an_item_reference_needs_an_id_or_a_key() {
        let empty = WorkItemRef {
            tracker: Some(PbTrackerRef::default()),
            ..Default::default()
        };
        assert!(require_item(Some(empty)).is_err());

        let by_key = WorkItemRef {
            key: "DEV-1".into(),
            ..Default::default()
        };
        assert!(require_item(Some(by_key)).is_ok());
    }
}
