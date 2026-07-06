//! Services: request/response facts that external callers drive by
//! inserting request entities directly into the bowl.
//!
//! A request is a plain entity carrying request components; a service system
//! answers by attaching a response component to the same entity. Callers use
//! `bowl.insert(...).bind().take::<Response>()` to await the answer and
//! consume the request entity in one step — an LSP adapter can map protocol
//! requests onto the bowl this way without a separate service layer.
//!
//! Services own the request components and the arbitration; the *content* of
//! an answer comes from the language entities via their service-stage traits
//! (see `lang::entity`).

mod hover;

pub(crate) use hover::{HoverInfo, HoverRequest, Position};

use bowl::Bowl;

pub(crate) async fn register_services(db: &Bowl) {
    db.add_system(hover::hover_info).await;
}
