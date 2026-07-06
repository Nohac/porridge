//! Syntax layer: lexer and lelwel-generated parser. This module only
//! produces the CST; language semantics live in `lang::entities`.

pub(crate) mod lexer;
pub(crate) mod parser;
