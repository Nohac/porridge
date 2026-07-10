//! Document entity: source files as they enter the bowl, and the parse that
//! turns their text into a CST.

use bowl::{Bowl, Commands, Component, DerivedFrom, Entity, Query};
use tracing::info;

use crate::lang::{
    entity::{AstFacts, HoverStage, LanguageEntity, LowerCtx, LowerStage},
    facts::Diagnostic,
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

    async fn register(db: &Bowl) {
        db.add_system(parse_file).await;
    }
}

impl LowerStage for Document {
    // The document owns the file root, but everything under it is lowered by
    // the entities owning the item rules — nothing to emit here.
    fn lower(_ctx: &LowerCtx<'_>, _node: NodeRef, _commands: &mut Commands<AstFacts>) {}
}

impl HoverStage for Document {
    // Documents carry no hover content; the service supplies the fallback.
    async fn register_hover(_db: &Bowl) {}
}

pub(crate) async fn parse_file(query: Query<(Entity, &FileText)>, mut commands: Commands<(ParsedFile, DerivedFrom, Diagnostic)>) {
    let (file, text) = query.item();

    crate::short_sleep().await;

    info!(entity = file.raw(), "parse_file");

    let mut diags = Vec::new();
    let cst = Parser::new(&text.0, &mut diags).parse(&mut diags);

    commands.entity(file).insert(ParsedFile {
        cst: cst.into_data(),
    });

    for diag in diags {
        commands.insert((DerivedFrom::new(file), Diagnostic(diag.message)));
    }
}
