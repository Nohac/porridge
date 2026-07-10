//! Document entity: source files as they enter the bowl, and the parse that
//! turns their text into a CST.

use bowl::{Commands, Component, DerivedFrom, Entity, Query, Registrar};
use tracing::info;

use crate::lang::{
    entity::{AstFacts, HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::{Diagnostic, Severity},
    grammar::parser::{CstData, NodeRef, Parser},
};

#[derive(Component, Hash, PartialEq, Eq)]
#[component(hash)]
pub(crate) struct FilePath(pub(crate) String);

#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct FileText(pub(crate) String);

#[derive(Component)]
pub(crate) struct ParsedFile {
    pub(crate) cst: CstData,
}

pub(crate) struct Document;

impl LanguageEntity for Document {
    const NAME: &'static str = "document";

    fn register(reg: &mut Registrar<'_>) {
        reg.system(parse_file);
    }
}

impl LowerStage for Document {
    // The document owns the file root, but everything under it is lowered by
    // the entities owning the item rules — nothing to emit here.
    fn lower(_ctx: &LowerCtx<'_>, _node: NodeRef, _commands: &mut Commands<AstFacts>) {}
}

impl HoverStage for Document {
    // Documents carry no hover content; the service supplies the fallback.
    fn register_hover(_reg: &mut Registrar<'_>) {}
}

pub(crate) async fn parse_file(
    query: Query<(Entity, &FileText)>,
    mut commands: Commands<(
        crate::lang::schema::lang_schema::ParsedFile,
        crate::lang::schema::lang_schema::Diagnostic,
    )>,
) {
    let (file, text) = query.item();

    crate::short_sleep().await;

    info!(entity = file.raw(), "parse_file");

    let mut diags = Vec::new();
    let cst = Parser::new(&text.0, &mut diags).parse(&mut diags);

    commands.entity(file).insert(ParsedFile {
        cst: cst.into_data(),
    });

    // Parse failures are errors: the schema's diagnostic shape made the
    // previously missing severity explicit.
    for diag in diags {
        commands.insert((
            DerivedFrom::new(file),
            Severity::Error,
            Diagnostic(diag.message),
        ));
    }
}
