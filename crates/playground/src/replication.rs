//! A dummy replicon-style plugin, dogfooding the plugin surface.
//!
//! It exercises both plugin patterns from the design:
//!
//! - **Own facts**: the plugin ships its schema fragment
//!   ([`ReplicationShapes`]) — replica records are the plugin's entity
//!   kind, and installing the plugin contributes the shapes and the
//!   systems atomically ([`bowl::Plugin`]).
//! - **Generic over app data**: the plugin never names an app type. The
//!   app subscribes its own *shapes*
//!   (`.replicate::<lang_schema::SourceFile>()`), which instantiates the
//!   generic tracking system per shape — the `emit_diagnostic`
//!   infectious-contract pattern scaled to a whole plugin.
//!
//! Replication is **shape-granular by design**: with an enforced schema,
//! component-granular replication could transit illegal partial entities
//! on the applying side. The protocol unit is a shape instance — the wire
//! analogue of strict spawning — so a replica either lands whole or not
//! at all, and the receiving bowl's conformance holds mid-stream.
//!
//! A real replicon would serialize the shape's parts over a wire from the
//! type-erased boundary (`changed_since` cursors on the capture side,
//! shape-bundle base writes on the apply side); the dummy stops at
//! maintaining the per-shape replica records such a wire would ship.
//! Capture is a facet query: `Entity<H>` anchors rows to entities
//! conforming to the shape, and `Tracked<H>` deps the row on every part,
//! so any change to the instance re-derives its record.

use bowl::{
    Commands, Component, DerivedFrom, Entity, FacetKind, Plugin, Query, Registrar, Schema,
    ShapeDesc,
    Tracked,
};

/// One replica record per replicated shape instance: what a wire protocol
/// would serialize. Derived from the source, so `cleanup_stale_derived`
/// (registered by the app's language plugin) reaps records whose source
/// changed or disappeared.
#[derive(Component, Hash)]
#[component(hash)]
pub(crate) struct Replica {
    pub(crate) source: Entity,
    /// The subscribed shape, standing in for a real protocol tag.
    pub(crate) shape: &'static str,
}

#[derive(bowl::Schema)]
pub(crate) struct ReplicationShapes {
    replica: (Replica, DerivedFrom),
}

/// One invocation per entity conforming to the subscribed shape;
/// `Tracked<H>` makes the row depend on every part of the instance, so
/// edits re-derive the record after cleanup reaps it. Idempotent reruns
/// keep their spawn slot, so an unchanged source keeps its replica's
/// identity and revisions.
async fn replicate_shape<H>(
    query: Query<(Entity<H>, Tracked<H>)>,
    mut commands: Commands<(replication_shapes::Replica,)>,
) where
    H: FacetKind,
{
    let (source, _tracked) = query.item();
    commands.insert((
        Replica {
            source: source.untyped(),
            shape: std::any::type_name::<H>(),
        },
        DerivedFrom::new(source),
    ));
}

/// The plugin: shape subscriptions are collected by the app at build
/// time, one generic system instantiation each.
pub(crate) struct ReplicationPlugin {
    subscriptions: Vec<Box<dyn Fn(&mut Registrar<'_>)>>,
}

impl ReplicationPlugin {
    pub(crate) fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
        }
    }

    /// Subscribes a schema shape: every entity conforming to `H` gets a
    /// replica record, kept current against every part of the shape.
    pub(crate) fn replicate<H>(mut self) -> Self
    where
        H: FacetKind,
    {
        self.subscriptions
            .push(Box::new(|reg| reg.system(replicate_shape::<H>)));
        self
    }
}

impl Plugin for ReplicationPlugin {
    fn shapes(&self) -> Vec<ShapeDesc> {
        ReplicationShapes::shapes()
    }

    fn build(&self, reg: &mut Registrar<'_>) {
        for subscription in &self.subscriptions {
            subscription(reg);
        }
    }
}
