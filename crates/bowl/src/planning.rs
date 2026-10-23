//! Reusable, driver-owned storage. Scratch never crosses a planning boundary.
use bump_scope::Bump;

use crate::{
    commands::Pool,
    world::{SystemId, SystemInvocation},
};

#[derive(Default)]
pub(crate) struct Storage {
    pub(crate) commands: Pool,
    pub(crate) scratch: Scratch,
}

pub(crate) struct Scratch {
    pub(crate) arena: Bump,
    pub(crate) owner: SystemInvocation,
}

impl Default for Scratch {
    fn default() -> Self {
        Self {
            arena: Bump::new(),
            owner: SystemInvocation {
                system: SystemId(0),
                keys: Vec::new(),
            },
        }
    }
}
