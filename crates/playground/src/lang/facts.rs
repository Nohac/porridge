//! Cross-cutting facts shared by every language entity: diagnostics, spans,
//! file anchoring, and the demand marker.

use bowl::{Commands, Component, DerivedFrom, Entity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Span {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct Diagnostic(pub(crate) String);

#[derive(Component, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[component(hash)]
pub(crate) enum Severity {
    Warning,
    Error,
}

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct BelongsToFile(pub(crate) Entity);

/// Demand marker (spec/language-entities.md): diagnostics systems gate on
/// this fact, so settles that nobody asked diagnostics from (a hover-only
/// request) never plan them. A preference, not a claim — only its owner
/// changes it, so it cannot go stale the way ordering markers can. (The
/// `AstAvailable`/`CstAvailable` *ordering* markers that used to live here
/// are gone: phases and tracked joins carry ordering now, and an emit-only
/// marker cycled off→on by its settled hook costs the whole bowl an extra
/// generation per settle.)
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct DiagnosticsDemand;

pub(crate) fn emit_diagnostic(
    commands: &mut Commands,
    derived_from: DerivedFrom,
    severity: Severity,
    message: impl Into<String>,
) {
    commands.insert((derived_from, severity, Diagnostic(message.into())));
}
