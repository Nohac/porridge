//! Shared fixtures for engine benchmarks.
//!
//! The components and systems here model the playground's shape (file inputs,
//! a derive pipeline, per-row checks with an ambient `View`) without the toy
//! language, so benchmark numbers isolate engine costs from parsing.

use bowl::{Bowl, Commands, Component, Cow, DerivedFrom, Entity, Eq, In, Query, View, Where};

#[derive(Component, Hash, Clone, PartialEq, std::cmp::Eq)]
#[component(hash)]
pub struct Path(pub String);

#[derive(Component, Hash, Clone)]
#[component(hash)]
pub struct Text(pub String);

#[derive(Component, Hash)]
#[component(hash)]
pub struct Parsed(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct FirstLine(pub String);

#[derive(Component, Hash)]
#[component(hash)]
pub struct Diag(pub String);

/// Filler component for scan benchmarks; never queried.
#[derive(Component)]
pub struct Padding(pub u64);

#[derive(Component, Hash, Clone)]
#[component(hash)]
pub struct Src(pub u64);

/// Derived from [`Src`]; stays value-identical when `Src` changes by +2.
#[derive(Component, Hash)]
#[component(hash)]
pub struct Parity(pub u64);

#[derive(Component, Hash, Clone)]
#[component(hash)]
pub struct Def(pub String);

pub async fn parse(query: Query<(Entity, &Text)>, mut commands: Commands<(Parsed,)>) {
    let (entity, text) = query.item();
    commands.entity(entity).insert(Parsed(text.0.len() as u64));
}

pub async fn extract_first_line(query: Query<(Entity, &Text)>, mut commands: Commands<(FirstLine,)>) {
    let (entity, text) = query.item();
    let line = text.0.lines().next().unwrap_or("").to_string();
    commands.entity(entity).insert(FirstLine(line));
}

pub async fn diag_long_files(query: Query<(Entity, &Parsed)>, mut commands: Commands<(DerivedFrom, Diag)>) {
    let (entity, parsed) = query.item();
    if parsed.0 > 60 {
        commands.insert((
            DerivedFrom::new(entity),
            Diag(format!("file too long: {} bytes", parsed.0)),
        ));
    }
}

pub async fn derive_parity(query: Query<(Entity, &Src)>, mut commands: Commands<(Parity,)>) {
    let (entity, src) = query.item();
    commands.entity(entity).insert(Parity(src.0 % 2));
}

pub async fn check_duplicate_defs(
    query: Query<(Entity, &Def)>,
    defs: View<'_, (Entity, &Def)>,
    mut commands: Commands<(DerivedFrom, Diag)>,
) {
    let (entity, def) = query.item();

    if let Some((previous, _)) = defs
        .iter()
        .find(|(other, other_def)| *other < entity && other_def.0 == def.0)
    {
        commands.insert((
            DerivedFrom::many([entity, previous]),
            Diag(format!("duplicate definition `{}`", def.0)),
        ));
    }
}

pub fn file_name(index: usize) -> String {
    format!("file_{index}.por")
}

/// Roughly half the files exceed the `diag_long_files` threshold, so reruns
/// exercise both plain component writes and derived entity spawns.
pub fn source_text(index: usize) -> String {
    if index % 2 == 0 {
        format!("fn short_{index}() {{}}")
    } else {
        format!("fn long_{index}() {{ return {index}; }} // padding padding padding")
    }
}

/// N file entities driving a three-system pipeline, not yet settled.
pub async fn file_pipeline_bowl(files: usize) -> Bowl {
    let bowl = Bowl::new();
    bowl.add_system(parse).await;
    bowl.add_system(extract_first_line).await;
    bowl.add_system(diag_long_files).await;

    for index in 0..files {
        bowl.insert((Path(file_name(index)), Text(source_text(index))))
            .await;
    }

    bowl
}

/// Settles the bowl by scooping a derived component.
pub async fn settle_files(bowl: &Bowl) -> usize {
    bowl.scoop::<Query<(Entity, &Parsed)>>().await.len()
}

/// Appends to one file's text, marking it dirty for the next generation.
pub async fn touch_file(bowl: &Bowl, index: usize) {
    bowl.scoop::<Query<(Entity, Cow<Text>), Where<Eq<Path>>>>()
        .args(Path(file_name(index)))
        .for_each(|(_, text)| text.0.push('x'))
        .await;
}

/// N `Src` rows plus the parity system, not yet settled. All sources start
/// even, so `Parity` is always `0`.
pub async fn parity_bowl(rows: usize) -> Bowl {
    let bowl = Bowl::new();
    bowl.add_system(derive_parity).await;

    for index in 0..rows {
        bowl.insert((Src(index as u64 * 2),)).await;
    }

    bowl
}

/// Changes every `Src` value while keeping every derived `Parity` identical.
pub async fn bump_all_sources(bowl: &Bowl) {
    bowl.scoop::<Query<(Entity, Cow<Src>)>>()
        .for_each(|(_, src)| src.0 += 2)
        .await;
}

/// Derived from [`Src`] onto a spawned entity; stays value-identical when
/// `Src` changes by +2.
#[derive(Component, Hash)]
#[component(hash)]
pub struct ParityNote(pub u64);

pub async fn spawn_parity_note(query: Query<(Entity, &Src)>, mut commands: Commands<(DerivedFrom, ParityNote)>) {
    let (entity, src) = query.item();
    commands.insert((DerivedFrom::new(entity), ParityNote(src.0 % 2)));
}

/// N `Src` rows plus a system that spawns one derived note per row.
pub async fn spawn_parity_bowl(rows: usize) -> Bowl {
    let bowl = Bowl::new();
    bowl.add_system(spawn_parity_note).await;

    for index in 0..rows {
        bowl.insert((Src(index as u64 * 2),)).await;
    }

    bowl
}

/// `padding` entities that never match target queries, plus `targets` file
/// entities. No systems.
pub async fn scan_bowl(padding: usize, targets: usize) -> Bowl {
    let bowl = Bowl::new();

    for index in 0..padding {
        bowl.insert((Padding(index as u64),)).await;
    }
    for index in 0..targets {
        bowl.insert((Path(file_name(index)), Text(source_text(index))))
            .await;
    }

    bowl
}

/// N unique defs driving the duplicate checker. Unique names mean no outputs:
/// the run measures pure planning/view/commit overhead.
pub async fn defs_bowl(defs: usize) -> Bowl {
    let bowl = Bowl::new();
    bowl.add_system(check_duplicate_defs).await;

    for index in 0..defs {
        bowl.insert((Def(format!("def_{index}")),)).await;
    }

    bowl
}

#[derive(Component, Hash)]
#[component(hash)]
#[relationship(target = GroupMembers)]
pub struct BelongsTo(pub Entity);

#[derive(Component)]
#[relationship_target(relationship = BelongsTo)]
pub struct GroupMembers(pub Vec<Entity>);

#[derive(Component, Hash)]
#[component(hash)]
pub struct GroupTag(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct Item(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct PairMark(pub u64);

pub async fn pair_items(
    groups: Query<(Entity, &GroupTag, &GroupMembers)>,
    member: Query<(Entity, &Item), Where<In<GroupMembers>>>,
    mut commands: Commands<(PairMark,)>,
) {
    let (_group, tag, _members) = groups.item();
    let (item_entity, item) = member.item();
    commands.entity(item_entity).insert(PairMark(tag.0 ^ item.0));
}

/// `groups` providers each holding `members_per_group` members, joined by a
/// `Where<In<..>>` system. Isolates join *planning* cost: the pair space is
/// providers × members-per-group, but naive planning probes providers ×
/// all-items tuples per wave.
pub async fn in_join_bowl(groups: usize, members_per_group: usize) -> (Bowl, Vec<Entity>) {
    let bowl = Bowl::new();
    bowl.add_system(pair_items).await;

    let mut group_entities = Vec::new();
    for group in 0..groups {
        let inserted = bowl.insert((GroupTag(group as u64),)).await;
        group_entities.push(inserted.entity());
        for member in 0..members_per_group {
            bowl.insert((
                Item((group * members_per_group + member) as u64),
                BelongsTo(inserted.entity()),
            ))
            .await;
        }
    }

    (bowl, group_entities)
}

/// Retags one group so exactly its pairs replan on the next settle.
pub async fn touch_group(bowl: &Bowl, group: Entity, bump: u64) {
    bowl.entity(group).insert((GroupTag(bump),)).await;
}
