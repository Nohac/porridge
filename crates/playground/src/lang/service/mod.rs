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
    HoverRank,
    CandidateParts, HoverCandidate, HoverFile, HoverInfo, HoverRequest, HoverWord, Position,
    RequestKey, priority,
};

use bowl::{Phase, Registrar, SystemExt};

pub(crate) fn register_services(reg: &mut Registrar<'_>) {
    // Enrichment reads only tracked inputs (request components plus the
    // FilePath outer join), so it runs in the default Evaluate phase;
    // candidate systems key on its outputs and replan as they commit. The
    // outer join runs unmatched requests too, so enrichment seeds the
    // whole fallback scaffold itself — no separate stamp system.
    reg.system(hover::resolve_hover_requests);
    // Arbitration is a tracked join over candidates with a monotone
    // priority upgrade, so it is same-phase-safe next to the candidate
    // systems in Complete — no barrier after them is needed.
    reg.system(hover::finalize_hover.run_during(Phase::Complete));
}
