//! Shared fixtures for engine benchmarks.
//!
//! The components and systems here model the playground's shape (file inputs,
//! a derive pipeline, per-row checks with an ambient `View`) without the toy
//! language, so benchmark numbers isolate engine costs from parsing.

use bowl::{Bowl, Commands, Component, Cow, DerivedFrom, Entity, Eq, In, Query, View, Where, With};

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

/// Wide-row fixtures for the presence-bitmap fast path: a three-part
/// tracked row, benchable on a schema bowl (mask matching) and a
/// schema-less one (per-part store probing) with identical data.
#[derive(Component, Hash, Clone)]
#[component(hash)]
pub struct W1(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct W2(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct W3(pub u64);

#[derive(bowl::Schema)]
pub struct WideSchema {
    wide: (W1, W2, W3),
}

/// N entities carrying the full wide row.
pub async fn wide_row_bowl(rows: usize, schema: bool) -> Bowl {
    let bowl = if schema {
        Bowl::builder().schema::<WideSchema>().build()
    } else {
        Bowl::builder().build()
    };
    for index in 0..rows {
        bowl.insert((W1(index as u64), W2(index as u64), W3(index as u64)))
            .await;
    }
    bowl
}

pub async fn parse(query: Query<(Entity, &Text)>, mut commands: Commands<(Parsed,)>) {
    let (entity, text) = query.item();
    commands.entity(entity).insert(Parsed(text.0.len() as u64));
}

pub async fn extract_first_line(query: Query<(Entity, &Text)>, mut commands: Commands<(FirstLine,)>) {
    let (entity, text) = query.item();
    let line = text.0.lines().next().unwrap_or("").to_string();
    commands.entity(entity).insert(FirstLine(line));
}

pub async fn diag_long_files(query: Query<(Entity, &Parsed)>, mut commands: Commands<((DerivedFrom, Diag),)>) {
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
    mut commands: Commands<((DerivedFrom, Diag),)>,
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
    let bowl = Bowl::builder()
        .system(parse)
        .system(extract_first_line)
        .system(diag_long_files)
        .build();

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
    let bowl = Bowl::builder()
        .system(derive_parity)
        .build();

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

pub async fn spawn_parity_note(query: Query<(Entity, &Src)>, mut commands: Commands<((DerivedFrom, ParityNote),)>) {
    let (entity, src) = query.item();
    commands.insert((DerivedFrom::new(entity), ParityNote(src.0 % 2)));
}

/// N `Src` rows plus a system that spawns one derived note per row.
pub async fn spawn_parity_bowl(rows: usize) -> Bowl {
    let bowl = Bowl::builder()
        .system(spawn_parity_note)
        .build();

    for index in 0..rows {
        bowl.insert((Src(index as u64 * 2),)).await;
    }

    bowl
}

/// `padding` entities that never match target queries, plus `targets` file
/// entities. No systems.
pub async fn scan_bowl(padding: usize, targets: usize) -> Bowl {
    let bowl = Bowl::builder().build();

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
    let bowl = Bowl::builder()
        .system(check_duplicate_defs)
        .build();

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
    let bowl = Bowl::builder()
        .system(pair_items)
        .build();

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

/// Fleet fixtures for planner gating: 32 systems over 32 disjoint
/// component types. A settle that touches one slot should plan one
/// system, not thirty-two.
pub struct Slot<const N: usize>(pub u64);

impl<const N: usize> Component for Slot<N> {}

pub async fn observe_slot<const N: usize>(query: Query<(Entity, &Slot<N>)>) {
    let (_entity, _slot) = query.item();
}

macro_rules! fleet_builder {
    ($($n:literal),*) => {{
        let builder = Bowl::builder();
        $(let builder = builder.system(observe_slot::<$n>);)*
        builder.build()
    }};
}

macro_rules! fleet_insert {
    ($bowl:expr, $rows:expr; $($n:literal),*) => {
        $(for index in 0..$rows {
            $bowl.insert((Slot::<$n>(index as u64),)).await;
        })*
    };
}

/// 32 slot systems, `rows` entities per slot, settled.
pub async fn fleet_bowl(rows: usize) -> (Bowl, Entity) {
    let bowl = fleet_builder!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
    fleet_insert!(bowl, rows; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
    let inserted = bowl.insert((Slot::<0>(999),)).await;
    let target = inserted.entity();
    bowl.scoop::<Query<(Entity, &Slot<0>)>>().await;
    (bowl, target)
}

/// Touches slot 0 only; 31 of 32 systems have no reason to plan.
pub async fn touch_slot0(bowl: &Bowl, target: Entity, bump: u64) {
    bowl.entity(target).insert((Slot::<0>(bump),)).await;
    bowl.scoop::<Query<(Entity, &Slot<0>)>>().await;
}

/// CPU-heavy fixtures for the parallel runtime: each row burns real
/// compute, so worker-thread spawning shows up as wall-clock division.
#[derive(Component, Hash, Clone)]
#[component(hash)]
pub struct Work(pub u64);

#[derive(Component, Hash)]
#[component(hash)]
pub struct Digest(pub u64);

pub async fn crunch(query: Query<(Entity, &Work)>, mut commands: Commands<(Digest,)>) {
    let (entity, work) = query.item();
    let mut digest = work.0;
    for _ in 0..120_000 {
        digest = digest
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
    }
    commands.entity(entity).insert(Digest(digest));
}

/// N compute rows, not yet settled.
pub async fn compute_bowl(rows: usize) -> Bowl {
    let bowl = Bowl::builder().system(crunch).build();
    for index in 0..rows {
        bowl.insert((Work(index as u64),)).await;
    }
    bowl
}

/// dsql-shaped fleet: every slot system pairs its row query with a tiny
/// demand gate and a singleton config — the multi-driver profile that
/// used to fall out of delta planning entirely.
#[derive(Component, Hash)]
#[component(hash)]
pub struct FleetDemand;

#[derive(Component, Hash)]
#[component(hash)]
pub struct FleetConfig(pub u64);

pub async fn observe_slot_gated<const N: usize>(
    demand: Query<Entity, With<FleetDemand>>,
    config: Query<(Entity, &FleetConfig)>,
    rows: Query<(Entity, &Slot<N>)>,
) {
    let _gate = demand.item();
    let (_config_entity, _config) = config.item();
    let (_entity, _slot) = rows.item();
}

macro_rules! gated_fleet_builder {
    ($($n:literal),*) => {{
        let builder = Bowl::builder();
        $(let builder = builder.system(observe_slot_gated::<$n>);)*
        builder.build()
    }};
}

/// 32 gated slot systems, `rows` entities per slot, settled.
pub async fn gated_fleet_bowl(rows: usize) -> (Bowl, Entity) {
    let bowl = gated_fleet_builder!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
    bowl.insert((FleetDemand,)).await;
    bowl.insert((FleetConfig(1),)).await;
    fleet_insert!(bowl, rows; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
    let inserted = bowl.insert((Slot::<0>(999),)).await;
    let target = inserted.entity();
    bowl.scoop::<Query<(Entity, &Slot<0>)>>().await;
    (bowl, target)
}
