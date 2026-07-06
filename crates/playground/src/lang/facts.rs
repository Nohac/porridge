//! Cross-cutting facts shared by every language entity: diagnostics, spans,
//! file anchoring, and the ephemeral settle markers.

use bowl::{Commands, Component, ComponentHookContext, DerivedFrom, Entity};
use tracing::info;

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

#[derive(Component)]
#[component(untracked)]
pub(crate) struct Ephemeral;

#[derive(Clone, Copy)]
pub(crate) struct CstAvailable;

impl Component for CstAvailable {
    fn tracked() -> bool {
        false
    }

    fn on_insert(context: ComponentHookContext) {
        info!(entity = context.entity().raw(), "CstAvailable insert");
    }

    fn on_remove(context: ComponentHookContext) {
        info!(entity = context.entity().raw(), "CstAvailable remove");
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AstAvailable;

impl Component for AstAvailable {
    fn tracked() -> bool {
        false
    }

    fn on_insert(context: ComponentHookContext) {
        info!(entity = context.entity().raw(), "AstAvailable insert");
    }

    fn on_remove(context: ComponentHookContext) {
        info!(entity = context.entity().raw(), "AstAvailable remove");
    }
}

pub(crate) fn emit_diagnostic(
    commands: &mut Commands,
    derived_from: DerivedFrom,
    severity: Severity,
    message: impl Into<String>,
) {
    commands.insert((derived_from, severity, Diagnostic(message.into())));
}
