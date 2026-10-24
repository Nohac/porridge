//! Stateful request workloads using the ordinary Bowl evaluation path.

pub mod churn;
pub mod purchases;
pub mod steady;
pub mod population;

#[cfg(test)]
mod tests;
