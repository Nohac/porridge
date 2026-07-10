use std::{
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    Bundle, Component, Entity,
    declare::{Anything, DeclaredIn, IncrementOf, SpawnsAs},
    entity::Untyped,
    world::{SystemInvocation, World},
};

/// Buffered writes issued by a system invocation.
///
/// Commands do not mutate the live world immediately. A system writes into its
/// invocation-local command buffer; the runner applies those commands at the
/// end of the generation.
///
/// ```text
/// system reads snapshot N
///   commands.entity(e).insert(X)
///
/// barrier
///   command applies to world N+1
/// ```
/// The type parameter is the system's *output declaration*: a tuple of
/// component types and/or group aliases (which are themselves tuple
/// aliases). `insert` bounds every bundle to the declared set, so emitting
/// an undeclared component does not compile. There is no default and no
/// public wildcard: every system declares, and `Commands<()>` marks a
/// removal-only writer. See `spec/declared-outputs.md`.
pub struct Commands<S> {
    pub(crate) inner: Arc<Mutex<CommandBuffer>>,
    _declares: PhantomData<fn() -> S>,
}

impl<S> Clone for Commands<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _declares: PhantomData,
        }
    }
}

/// Invocation-local buffer state: the queued operations plus the spawn-id
/// reservation context.
pub(crate) struct CommandBuffer {
    ops: Vec<Box<dyn CommandOp>>,
    /// The invocation's previous spawn ids, in spawn order. Reservation
    /// hands these out slot by slot so idempotent reruns keep their entity
    /// identity; new slots allocate from the shared counter.
    spawn_slots: Vec<Entity>,
    spawn_cursor: usize,
    allocator: Arc<AtomicU64>,
}

impl Commands<Anything> {
    /// Creates an invocation-local command buffer that reserves spawn ids
    /// from `spawn_slots` first and `allocator` after.
    pub(crate) fn new(spawn_slots: Vec<Entity>, allocator: Arc<AtomicU64>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CommandBuffer {
                ops: Vec::new(),
                spawn_slots,
                spawn_cursor: 0,
                allocator,
            })),
            _declares: PhantomData,
        }
    }
}

impl<S> Commands<S> {
    /// Reinterprets the shared buffer under a different declaration. The
    /// runner owns one wildcard buffer per invocation; each `Commands<S>`
    /// param is a typed view of it.
    pub(crate) fn retype<S2>(&self) -> Commands<S2> {
        Commands {
            inner: Arc::clone(&self.inner),
            _declares: PhantomData,
        }
    }

    /// Returns a builder for writing components to an existing entity.
    ///
    /// The handle's facet carries into the builder: through `Entity<H>`
    /// only the shape's `Option<T>` parts may be inserted (required parts
    /// are complete at spawn), while the untyped handle keeps plain
    /// membership semantics against `S`.
    pub fn entity<H>(&mut self, entity: Entity<H>) -> EntityCommands<'_, S, H> {
        EntityCommands {
            commands: self,
            entity: entity.untyped(),
            _facet: PhantomData,
        }
    }

    /// Buffers spawning a new derived entity with `bundle` and returns its
    /// reserved id, so sibling commands in the same buffer can link to it
    /// (parent/child facts during lowering).
    ///
    /// Spawning is *strict*: the bundle must fully match one shape declared
    /// in `S` — every required (non-`Option`) part present, nothing outside
    /// the shape — and the returned handle is typed with the matched facet,
    /// `Entity<Shape>`. Partial entities cannot be spawned; optional parts
    /// are added later through `entity(..)`.
    ///
    /// Reservation reuses the invocation's previous spawn ids slot by slot,
    /// so idempotent reruns keep their entity identity; only genuinely new
    /// slots allocate. The id is guaranteed only for fresh spawns: a
    /// `Singleton<T>` bundle resolves to the already-existing singleton
    /// entity when the buffer applies.
    pub fn insert<B, M>(&mut self, bundle: B) -> Entity<B::Shape>
    where
        B: Bundle + SpawnsAs<S, M>,
    {
        let mut buffer = self.inner.lock().expect("command buffer lock poisoned");
        let reserved = buffer
            .spawn_slots
            .get(buffer.spawn_cursor)
            .copied()
            .unwrap_or_else(|| Entity::from_raw(buffer.allocator.fetch_add(1, Ordering::Relaxed)));
        buffer.spawn_cursor += 1;
        buffer.ops.push(Box::new(SpawnCommand { bundle, reserved }));
        reserved.retype()
    }

    /// Buffers removing an entity and all attached components.
    pub fn remove<H>(&mut self, entity: Entity<H>) {
        self.inner
            .lock()
            .expect("command buffer lock poisoned")
            .ops
            .push(Box::new(RemoveEntityCommand {
                entity: entity.untyped(),
            }));
    }

    /// Drains buffered command operations after the system invocation returns.
    pub(crate) fn take(self) -> Vec<Box<dyn CommandOp>> {
        std::mem::take(
            &mut self
                .inner
                .lock()
                .expect("command buffer lock poisoned")
                .ops,
        )
    }
}

/// Command builder scoped to one entity. The facet parameter `H` bounds
/// what may be written: `Untyped` (the default) means plain declaration
/// membership, a shape facet restricts inserts to its optional parts.
pub struct EntityCommands<'a, S, H = Untyped> {
    commands: &'a mut Commands<S>,
    entity: Entity,
    _facet: PhantomData<fn() -> H>,
}

impl<S, H> EntityCommands<'_, S, H> {
    /// Buffers insertion of a derived component on this entity.
    ///
    /// The component is owned by the current system invocation. When that
    /// invocation reruns, previous derived outputs with the same owner are
    /// removed before the new commands are applied. The component must be
    /// declared in `S` and, through a facet handle, be an `Option<T>` part
    /// of the facet's shape.
    pub fn insert<T, M, M2>(&mut self, value: T)
    where
        T: Component + DeclaredIn<S, M> + IncrementOf<H, M2>,
    {
        self.commands
            .inner
            .lock()
            .expect("command buffer lock poisoned")
            .ops
            .push(Box::new(InsertCommand {
                entity: self.entity,
                value,
            }));
    }

    /// Buffers removing component `T` from this entity.
    pub fn remove<T: Component>(&mut self) {
        self.commands
            .inner
            .lock()
            .expect("command buffer lock poisoned")
            .ops
            .push(Box::new(RemoveComponentCommand::<T> {
                entity: self.entity,
                _marker: std::marker::PhantomData,
            }));
    }
}

/// Operation produced by a system command buffer.
pub(crate) trait CommandOp: Send {
    /// Applies the operation as a derived write owned by `owner`.
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation);

    /// Whether this operation is held back when issued from a
    /// [`Phase::Settle`](crate::Phase::Settle) system: inserts and spawns
    /// queue as inputs for the next run, removals apply within the settle.
    fn defers_at_settle(&self) -> bool {
        false
    }
}

struct InsertCommand<T> {
    entity: Entity,
    value: T,
}

impl<T: Component> CommandOp for InsertCommand<T> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        world.insert_derived(self.entity, self.value, owner.clone());
    }

    fn defers_at_settle(&self) -> bool {
        true
    }
}

struct SpawnCommand<B> {
    bundle: B,
    /// Reserved at buffer time so sibling commands could link to it. A
    /// singleton bundle supersedes the reservation with the existing
    /// singleton entity when there is one.
    reserved: Entity,
}

impl<B: Bundle> CommandOp for SpawnCommand<B> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        let entity = B::singleton_key()
            .map(|key| world.singleton_entity_or_register(key, self.reserved))
            .unwrap_or(self.reserved);
        world.record_derived_spawn(owner, entity);
        self.bundle.insert_derived(world, entity, owner.clone());
    }

    fn defers_at_settle(&self) -> bool {
        true
    }
}

struct RemoveEntityCommand {
    entity: Entity,
}

impl CommandOp for RemoveEntityCommand {
    fn apply(self: Box<Self>, world: &mut World, _owner: &SystemInvocation) {
        world.remove_entity(self.entity);
    }
}

struct RemoveComponentCommand<T> {
    entity: Entity,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: Component> CommandOp for RemoveComponentCommand<T> {
    fn apply(self: Box<Self>, world: &mut World, _owner: &SystemInvocation) {
        world.remove_component::<T>(self.entity);
    }
}

#[doc(hidden)]
pub trait BaseCommandOp: Send {
    /// Applies the operation as a base input write.
    fn apply(self: Box<Self>, world: &mut World);
}

/// Base insert queued by public `Bowl::insert`.
pub(crate) struct InsertBaseCommand<T> {
    pub(crate) entity: Entity,
    pub(crate) value: T,
}

impl<T: Component> BaseCommandOp for InsertBaseCommand<T> {
    fn apply(self: Box<Self>, world: &mut World) {
        world.insert_base(self.entity, self.value);
    }
}

/// Base removal queued by external `bowl.entity(..).remove::<T>()`.
pub(crate) struct RemoveComponentBaseCommand<T> {
    pub(crate) entity: Entity,
    pub(crate) _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: Component> BaseCommandOp for RemoveComponentBaseCommand<T> {
    fn apply(self: Box<Self>, world: &mut World) {
        world.remove_component::<T>(self.entity);
    }
}
