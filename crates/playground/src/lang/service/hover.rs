//! Hover service: request components, request enrichment, and arbitration
//! of the candidate facts the language entities contribute.
//!
//! The pipeline needs exactly one phase barrier, and arbitration is a
//! commutative fold, not call order:
//!
//! 1. `stamp_hover_requests` + `resolve_hover_requests` (Evaluate) read
//!    only tracked inputs, so they need no ordering at all. Stamping seeds
//!    the answer scaffold — [`RequestKey`], [`HoverRank`] at zero, and the
//!    lowest-rank [`HoverInfo`] fallback. Resolution is a bound join (the
//!    request's `FilePath` pairs with the file carrying the equal path); a
//!    resolved request gets [`HoverFile`]/[`HoverWord`] plus a word-aware
//!    fallback upgrade.
//! 2. Each entity's own hover system (Complete, registered through
//!    `HoverStage::register_hover`) reads the enriched request plus its own
//!    facts *ambiently* and inserts [`HoverCandidate`] facts. The ambient
//!    reads of Evaluate output are why these sit in Complete: the phase
//!    boundary is the barrier that makes `View`s of lowered facts
//!    deterministic.
//! 3. `finalize_hover` (also Complete) consumes candidates *tracked*: a
//!    [`RequestKey`] join yields one invocation per (request, candidate)
//!    pair, and each pair monotonically upgrades the request's
//!    [`HoverRank`]/[`HoverInfo`] when its candidate outranks the current
//!    answer. A max-fold commutes, so pair order does not matter, and
//!    tracked consumption means candidates committing later replan the
//!    pair — same-phase-safe next to the candidate systems, no further
//!    barrier needed. By settle, the highest-priority candidate has won.
//!
//! Earlier versions gated enrichment on the ephemeral `AstAvailable`
//! marker (racy: marker-gated work is invisible to settledness checks) and
//! then picked the winner ambiently in the settle phase (impossible since
//! settle-phase inserts defer to the next run). Tracked joins dissolve the
//! ordering problem outright.

use bowl::{Commands, Component, Entity, Eq, MutRef, Query, Where, With};
use tracing::info;

use crate::lang::entities::document::{FilePath, FileText};

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverRequest;

#[derive(Debug, Component, Hash)]
#[component(hash)]
pub(crate) struct Position {
    pub(crate) offset: usize,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverInfo(pub(crate) String);

/// The request's own id as a join key: candidates carry an equal key, so
/// arbitration pairs each request with exactly its own candidates.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct RequestKey(pub(crate) Entity);

/// Priority of the request's current [`HoverInfo`]. Answers only ever
/// upgrade (strictly greater), which makes arbitration order-independent.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverRank(pub(crate) u8);

/// The file a hover request resolved to, stamped by enrichment.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverFile(pub(crate) Entity);

/// The word under the cursor, stamped by enrichment when one exists.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverWord(pub(crate) String);

/// One entity's answer for one hover request, addressed by an equal
/// [`RequestKey`]; see [`priority`] for the bands.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverCandidate {
    pub(crate) priority: u8,
    pub(crate) text: String,
}

/// Priority bands for hover answers: the more position-specific, the
/// higher. The zero and fallback bands are the enrichment scaffold every
/// candidate outranks.
pub(crate) mod priority {
    /// Matched the exact span under the cursor (imports).
    pub(crate) const POSITION: u8 = 30;
    /// Matched the word against a namespace-qualified definition.
    pub(crate) const QUALIFIED_NAME: u8 = 20;
    /// Matched the word against a plain definition.
    pub(crate) const NAME: u8 = 10;
    /// The request resolved to a file but no candidate answered.
    pub(crate) const RESOLVED: u8 = 1;
    /// Initial scaffold: the request did not even resolve to a file.
    pub(crate) const NONE: u8 = 0;
}

pub(crate) async fn stamp_hover_requests(
    query: Query<(Entity, &Position), With<HoverRequest>>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, _position) = query.item();
    info!(request = request.raw(), "stamp_hover_requests");
    commands.entity(request).insert(RequestKey(request));
    commands.entity(request).insert(HoverRank(priority::NONE));
    commands
        .entity(request)
        .insert(HoverInfo("unknown file".to_string()));
}

/// Join: the request's `FilePath` binds to the file entity carrying the
/// equal path, making the file lookup a planned pair instead of a view
/// scan — one invocation per (request, matching file). A pair existing at
/// all means the file resolved, so this also upgrades the fallback answer
/// past the "unknown file" scaffold.
pub(crate) async fn resolve_hover_requests(
    query: Query<
        (
            Entity,
            &FilePath,
            &Position,
            MutRef<'_, HoverRank>,
            MutRef<'_, HoverInfo>,
        ),
        With<HoverRequest>,
    >,
    file: Query<(Entity, &FileText), Where<Eq<FilePath>>>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, _path, position, mut rank, mut info) = query.item();
    let (file_entity, text) = file.item();
    info!(
        request = request.raw(),
        file = file_entity.raw(),
        "resolve_hover_requests"
    );

    commands.entity(request).insert(HoverFile(file_entity));
    let word = word_at(&text.0, position.offset);
    if let Some(word) = word {
        commands.entity(request).insert(HoverWord(word.to_string()));
    }

    if priority::RESOLVED > rank.0 {
        rank.0 = priority::RESOLVED;
        info.0 = match word {
            Some(word) => format!("unresolved symbol `{word}`"),
            None => "no symbol at position".to_string(),
        };
    }
}

/// Arbitration: one invocation per (request, candidate) pair via the
/// [`RequestKey`] join, each monotonically upgrading the request's answer
/// when its candidate outranks the current one. A max-fold commutes, so
/// pair order is irrelevant, and tracked consumption replans the pair when
/// candidates commit — no phase barrier after the candidate systems.
pub(crate) async fn finalize_hover(
    query: Query<
        (
            Entity,
            &RequestKey,
            MutRef<'_, HoverRank>,
            MutRef<'_, HoverInfo>,
        ),
        With<HoverRequest>,
    >,
    candidate: Query<(Entity, &HoverCandidate), Where<Eq<RequestKey>>>,
) {
    crate::short_sleep().await;

    let (request, _key, mut rank, mut info) = query.item();
    let (_candidate_entity, candidate) = candidate.item();

    if candidate.priority > rank.0 {
        rank.0 = candidate.priority;
        info.0 = candidate.text.clone();
        info!(request = request.raw(), message = %info.0, "finalize_hover upgrade");
    }
}

fn word_at(text: &str, offset: usize) -> Option<&str> {
    if offset >= text.len() || !text.is_char_boundary(offset) {
        return None;
    }

    let is_word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let bytes = text.as_bytes();

    if !is_word(bytes[offset]) {
        return None;
    }

    let start = bytes[..offset]
        .iter()
        .rposition(|byte| !is_word(*byte))
        .map_or(0, |index| index + 1);
    let end = bytes[offset..]
        .iter()
        .position(|byte| !is_word(*byte))
        .map_or(text.len(), |index| offset + index);

    Some(&text[start..end])
}
