//! Hover service: request components, request enrichment, and finalization
//! of the candidate facts the language entities contribute.
//!
//! The pipeline is ordered by *phases*, not gate markers:
//!
//! 1. `stamp_hover_requests` + `resolve_hover_requests` (Complete) mark the
//!    request enriched and resolve its file and the word under the cursor.
//!    `Phase::Complete` runs after Evaluate has converged in every
//!    generation, so the AST facts the pipeline reads are always consistent
//!    with the generation's inputs — including a request batched together
//!    with the source it asks about. File resolution is a bound join: the
//!    request's `FilePath` pairs with the file carrying the equal path.
//! 2. Each entity's own hover system (Complete, registered through
//!    `HoverStage::register_hover`) reads the enriched request plus its own
//!    facts and inserts [`HoverCandidate`] facts. In-phase streaming plans
//!    them as soon as enrichment commits.
//! 3. `finalize_hover` (Cleanup) picks the highest-priority candidate — by
//!    settle time every Complete wave has run, so every candidate exists —
//!    and writes [`HoverInfo`] onto the request. It is driven by
//!    [`HoverEnriched`], not the raw request, so it cannot answer before
//!    enrichment ran. Arbitration is data, not call order.
//!
//! An earlier version gated enrichment on the ephemeral `AstAvailable`
//! marker instead. That was racy by construction: work gated on a marker is
//! invisible to every settledness check while the marker is absent, so
//! concurrent settles could declare the bowl settled between the marker's
//! cleanup and its next re-emission, starving requests that arrived in that
//! window. Phase boundaries give the same ordering per generation with no
//! window at all.

use bowl::{Commands, Component, Entity, Eq, Query, SystemParam, View, Where, With};
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

/// Marker stamped on every hover request, resolvable or not. Downstream
/// systems key on enrichment outputs so phase ordering covers them all.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverEnriched;

/// The file a hover request resolved to, stamped by enrichment.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverFile(pub(crate) Entity);

/// The word under the cursor, stamped by enrichment when one exists.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverWord(pub(crate) String);

/// One entity's answer for one hover request. The finalizer picks the
/// highest priority; see [`priority`] for the bands.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct HoverCandidate {
    pub(crate) request: Entity,
    pub(crate) priority: u8,
    pub(crate) text: String,
}

/// Priority bands for hover candidates: the more position-specific the
/// answer, the higher it ranks.
pub(crate) mod priority {
    /// Matched the exact span under the cursor (imports).
    pub(crate) const POSITION: u8 = 30;
    /// Matched the word against a namespace-qualified definition.
    pub(crate) const QUALIFIED_NAME: u8 = 20;
    /// Matched the word against a plain definition.
    pub(crate) const NAME: u8 = 10;
}

pub(crate) async fn stamp_hover_requests(
    query: Query<(Entity, &Position), With<HoverRequest>>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, _position) = query.item();
    info!(request = request.raw(), "stamp_hover_requests");
    commands.entity(request).insert(HoverEnriched);
}

/// Join: the request's `FilePath` binds to the file entity carrying the
/// equal path, making the file lookup a planned pair instead of a view
/// scan — one invocation per (request, matching file).
pub(crate) async fn resolve_hover_requests(
    query: Query<(Entity, &FilePath, &Position), With<HoverRequest>>,
    file: Query<(Entity, &FileText), Where<Eq<FilePath>>>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, _path, position) = query.item();
    let (file_entity, text) = file.item();
    info!(
        request = request.raw(),
        file = file_entity.raw(),
        "resolve_hover_requests"
    );

    commands.entity(request).insert(HoverFile(file_entity));
    if let Some(word) = word_at(&text.0, position.offset) {
        commands.entity(request).insert(HoverWord(word.to_string()));
    }
}

/// Everything the finalizer reads, grouped as a param bundle so the
/// signature stays flat as entities grow (spec/language-entities.md).
#[derive(SystemParam)]
pub(crate) struct HoverOutcome<'a> {
    candidates: View<'a, (Entity, &'a HoverCandidate)>,
    resolution: HoverResolution<'a>,
}

/// Nested bundle: the enrichment outputs.
#[derive(SystemParam)]
pub(crate) struct HoverResolution<'a> {
    files: View<'a, (Entity, &'a HoverFile)>,
    words: View<'a, (Entity, &'a HoverWord)>,
}

pub(crate) async fn finalize_hover(
    query: Query<(Entity, &HoverEnriched), With<HoverRequest>>,
    outcome: HoverOutcome<'_>,
    mut commands: Commands,
) {
    crate::short_sleep().await;

    let (request, _enriched) = query.item();

    if !outcome
        .resolution
        .files
        .iter()
        .any(|(entity, _)| entity == request)
    {
        commands
            .entity(request)
            .insert(HoverInfo("unknown file".to_string()));
        return;
    }

    let best = outcome
        .candidates
        .iter()
        .filter(|(_, candidate)| candidate.request == request)
        .max_by_key(|(_, candidate)| candidate.priority)
        .map(|(_, candidate)| candidate.text.clone());

    let message = best.unwrap_or_else(|| {
        match outcome
            .resolution
            .words
            .iter()
            .find(|(entity, _)| *entity == request)
        {
            Some((_, word)) => format!("unresolved symbol `{}`", word.0),
            None => "no symbol at position".to_string(),
        }
    });
    info!(request = request.raw(), message = %message, "finalize_hover answer");
    commands.entity(request).insert(HoverInfo(message));
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
