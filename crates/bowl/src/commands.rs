use std::sync::{Arc, Mutex};

use crate::{
    Bundle, Component, Entity,
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
#[derive(Clone)]
pub struct Commands {
    pub(crate) inner: Arc<Mutex<Vec<Box<dyn CommandOp>>>>,
}

impl Commands {
    /// Creates an empty invocation-local command buffer.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a builder for writing components to an existing entity.
    pub fn entity(&mut self, entity: Entity) -> EntityCommands<'_> {
        EntityCommands {
            commands: self,
            entity,
        }
    }

    /// Buffers spawning a new derived entity with `bundle`.
    pub fn insert<B: Bundle>(&mut self, bundle: B) {
        self.inner
            .lock()
            .expect("command buffer lock poisoned")
            .push(Box::new(SpawnCommand { bundle }));
    }

    /// Buffers removing an entity and all attached components.
    pub fn remove(&mut self, entity: Entity) {
        self.inner
            .lock()
            .expect("command buffer lock poisoned")
            .push(Box::new(RemoveEntityCommand { entity }));
    }

    /// Drains buffered command operations after the system invocation returns.
    pub(crate) fn take(self) -> Vec<Box<dyn CommandOp>> {
        std::mem::take(&mut *self.inner.lock().expect("command buffer lock poisoned"))
    }
}

/// Command builder scoped to one entity.
pub struct EntityCommands<'a> {
    commands: &'a mut Commands,
    entity: Entity,
}

impl EntityCommands<'_> {
    /// Buffers insertion of a derived component on this entity.
    ///
    /// The component is owned by the current system invocation. When that
    /// invocation reruns, previous derived outputs with the same owner are
    /// removed before the new commands are applied.
    pub fn insert<T: Component>(&mut self, value: T) {
        self.commands
            .inner
            .lock()
            .expect("command buffer lock poisoned")
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
}

struct InsertCommand<T> {
    entity: Entity,
    value: T,
}

impl<T: Component> CommandOp for InsertCommand<T> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        world.insert_derived(self.entity, self.value, owner.clone());
    }
}

struct SpawnCommand<B> {
    bundle: B,
}

impl<B: Bundle> CommandOp for SpawnCommand<B> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        let entity = B::singleton_key()
            .map(|key| world.singleton_entity_or_spawn(key))
            .unwrap_or_else(|| world.spawn_empty());
        self.bundle.insert_derived(world, entity, owner.clone());
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
