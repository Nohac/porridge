//! Isolated comparison of the same observable purchase contract.
pub mod bevy;
pub mod porridge;

pub use fixtures::stateful::purchases::{Purchase, Response};

#[derive(Debug, PartialEq, Eq)]
pub struct AccountState {
    pub owner: usize,
    pub balance: u64,
    pub units: u64,
    pub capacity: u64,
    pub sequence: u64,
}

#[cfg(test)]
mod tests;
