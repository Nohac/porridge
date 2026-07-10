//! Output declarations for [`Commands`](crate::Commands): type-level
//! membership so `Commands<S>` can only emit components declared in `S`,
//! plus runtime enumeration of the declared set for the system graph.
//!
//! A declaration `S` is a tuple mixing bare component types and *groups* —
//! which are just tuple type aliases, closed and nestable:
//!
//! ```text
//! type DiagnosticParts = (Diagnostic, Severity, DerivedFrom);
//! Commands<(DiagnosticParts, Span)>
//! ```
//!
//! Membership is proved structurally: a component is its own singleton
//! group (reflexivity), tuples are searched head/tail with marker types
//! recording the path (the same disambiguation trick system functions
//! use), and `Option<T>` in declaration position declares `T` ("may
//! emit"). A component reachable through two declared items makes the
//! proof ambiguous ("type annotations needed") — keep groups disjoint.

use std::any::TypeId;

use crate::Component;
use variadics_please::all_tuples;

/// Wildcard output declaration: the system may emit anything. The default
/// for bare `Commands`, and a wildcard edge in the dependency graph.
pub struct Anything;

/// Membership-proof marker: matched by reflexivity.
pub struct ExactMatch;
/// Membership-proof marker: matched the head of a declaration tuple.
pub struct Here<M>(M);
/// Membership-proof marker: matched in the tail of a declaration tuple.
pub struct There<M>(M);
/// Membership-proof marker: matched inside `Option<..>`.
pub struct Optionally<M>(M);
/// Membership-proof marker: matched the wildcard.
pub struct WildcardMatch;

/// Component `Self` is declared in `S` (proof path recorded by `M`).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not declared in this `Commands<..>` output set",
    label = "undeclared component",
    note = "add `{Self}` (or a group containing it) to the `Commands<S>` declaration"
)]
pub trait DeclaredIn<S, M> {}

impl<C: Component> DeclaredIn<C, ExactMatch> for C {}

impl<C: Component> DeclaredIn<Anything, WildcardMatch> for C {}

impl<C, T, M> DeclaredIn<Option<T>, Optionally<M>> for C where C: DeclaredIn<T, M> {}

macro_rules! impl_declared_in_tuple {
    ($H:ident $(, $T:ident)*) => {
        impl<C, $H, $($T,)* M> DeclaredIn<($H, $($T,)*), Here<M>> for C
        where
            C: DeclaredIn<$H, M>,
        {
        }

        impl<C, $H, $($T,)* M> DeclaredIn<($H, $($T,)*), There<M>> for C
        where
            C: DeclaredIn<($($T,)*), M>,
        {
        }
    };
}

all_tuples!(impl_declared_in_tuple, 1, 8, T);

/// Every component of bundle `Self` is declared in `S`.
#[diagnostic::on_unimplemented(
    message = "bundle `{Self}` contains a component not declared in this `Commands<..>` output set",
    label = "bundle with undeclared component"
)]
pub trait BundleDeclaredIn<S, M> {}

macro_rules! impl_bundle_declared_in {
    ($(($C:ident, $M:ident)),*) => {
        impl<S, $($C,)* $($M,)*> BundleDeclaredIn<S, ($($M,)*)> for ($($C,)*)
        where
            $($C: DeclaredIn<S, $M>,)*
        {
        }
    };
}

all_tuples!(impl_bundle_declared_in, 1, 8, C, M);

/// Runtime enumeration of a declaration: the component `TypeId`s it
/// covers, or `None` for the wildcard. This is what makes tuple-alias
/// groups usable by the dependency graph — a closed tuple type can be
/// walked, unlike distributed trait impls.
pub trait DeclarationList {
    fn declared_types() -> Option<Vec<TypeId>>;
}

impl DeclarationList for Anything {
    fn declared_types() -> Option<Vec<TypeId>> {
        None
    }
}

/// The empty declaration: a removal-only writer.
impl DeclarationList for () {
    fn declared_types() -> Option<Vec<TypeId>> {
        Some(Vec::new())
    }
}

impl<C: Component> DeclarationList for C {
    fn declared_types() -> Option<Vec<TypeId>> {
        Some(vec![TypeId::of::<C>()])
    }
}

impl<T: DeclarationList> DeclarationList for Option<T> {
    fn declared_types() -> Option<Vec<TypeId>> {
        T::declared_types()
    }
}

macro_rules! impl_declaration_list_tuple {
    ($($T:ident),*) => {
        impl<$($T: DeclarationList,)*> DeclarationList for ($($T,)*) {
            fn declared_types() -> Option<Vec<TypeId>> {
                let mut out = Vec::new();
                $(
                    match $T::declared_types() {
                        Some(types) => out.extend(types),
                        // A wildcard anywhere makes the whole declaration
                        // a wildcard (conservative).
                        None => return None,
                    }
                )*
                Some(out)
            }
        }
    };
}

all_tuples!(impl_declaration_list_tuple, 1, 8, T);
