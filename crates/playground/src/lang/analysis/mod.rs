mod checks;
mod parser;

pub(crate) use checks::{check_duplicate_defs, check_imports};
pub(crate) use parser::{generate_ast, parse_file};
