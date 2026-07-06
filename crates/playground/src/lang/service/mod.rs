//! Services: request/response facts that external callers drive by
//! inserting request entities directly into the bowl.
//!
//! A request is a plain entity carrying request components; callers use
//! `bowl.insert(...).bind().take::<Response>()` to await the answer and
//! consume the request entity in one step — an LSP adapter can map protocol
//! requests onto the bowl this way without a separate service layer.
//!
//! Services own the request components, the request enrichment, and the
//! finalization; the *content* of an answer comes from the language entities
//! as candidate facts emitted by their own systems (see the pipeline
//! description in `hover`).

mod hover;

pub(crate) use hover::{
    HoverCandidate, HoverFile, HoverInfo, HoverRequest, HoverWord, Position, priority,
};

use bowl::{Bowl, Phase, SystemExt};

pub(crate) async fn register_services(db: &Bowl) {
    db.add_system(hover::stamp_hover_requests.run_during(Phase::Complete))
        .await;
    db.add_system(hover::resolve_hover_requests.run_during(Phase::Complete))
        .await;
    db.add_system(hover::finalize_hover.run_during(Phase::Cleanup))
        .await;
}
