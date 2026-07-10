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

/// Wildcard output declaration, crate-internal only: the runner's raw
/// per-invocation buffer and engine tests. There is no public wildcard —
/// every system declares its output set.
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

/// Membership-proof marker: a required shape part found in the bundle.
pub struct RequiredHere<M>(M);
/// Membership-proof marker: an optional shape part, exempt from presence.
pub struct OptionalSkip;

/// Shape part `Self` imposes its presence requirement on bundle `B`:
/// required parts must appear in the bundle, `Option<T>` parts are exempt.
pub trait RequiredPartIn<B, M> {}

impl<T, B> RequiredPartIn<B, OptionalSkip> for Option<T> {}

impl<C: Component, B, M> RequiredPartIn<B, RequiredHere<M>> for C where C: DeclaredIn<B, M> {}

/// All required parts of shape `Self` are present in bundle `B` — the
/// completeness half of strict spawn matching. (The membership half —
/// bundle ⊆ shape — is [`BundleDeclaredIn`] with the shape as the set.)
pub trait CoveredBy<B, M> {}

/// A bare component as a degenerate one-part shape.
impl<C: Component, B, M> CoveredBy<B, RequiredHere<M>> for C where C: DeclaredIn<B, M> {}

macro_rules! impl_covered_by {
    ($(($P:ident, $M:ident)),*) => {
        impl<B, $($P,)* $($M,)*> CoveredBy<B, ($($M,)*)> for ($($P,)*)
        where
            $($P: RequiredPartIn<B, $M>,)*
        {
        }
    };
}

all_tuples!(impl_covered_by, 1, 8, P, M);

/// Bundle `Self` *matches* shape `H`: every bundle component is part of
/// the shape, and every required part of the shape is in the bundle.
#[diagnostic::on_unimplemented(
    message = "bundle `{Self}` does not match shape `{H}`",
    label = "bundle/shape mismatch",
    note = "a spawn bundle must carry every required (non-`Option`) part of the shape and nothing outside it"
)]
pub trait MatchesShape<H, M> {}

impl<B, H, M1, M2> MatchesShape<H, (M1, M2)> for B
where
    B: BundleDeclaredIn<H, M1>,
    H: CoveredBy<B, M2>,
{
}

/// Bundle `Self` matches exactly one shape declared in `S`; `Shape` names
/// it. This is what makes spawns *strict*: `Commands<S>::insert` requires
/// the bundle to be a complete instance of one declared shape (membership
/// alone no longer admits partial entities), and returns the typed handle
/// `Entity<Shape>` for it.
///
/// Uniqueness is by inference: a bundle matching two declared shapes makes
/// the proof ambiguous ("type annotations needed") — keep shapes disjoint.
#[diagnostic::on_unimplemented(
    message = "bundle `{Self}` does not match any shape declared in this `Commands<..>` output set",
    label = "no matching shape",
    note = "spawns must fully match one declared shape: all required parts present, nothing undeclared; declare shapes as tuples (`Commands<((A, B, Option<C>),)>`) or schema aliases (`Commands<(my_schema::Thing,)>`)"
)]
pub trait SpawnsAs<S, M> {
    /// The matched shape, i.e. the facet of the returned `Entity<Shape>`.
    type Shape;
}

macro_rules! impl_spawns_as_tuple {
    ($H:ident $(, $T:ident)*) => {
        impl<B, $H, $($T,)* M> SpawnsAs<($H, $($T,)*), Here<M>> for B
        where
            B: MatchesShape<$H, M>,
        {
            type Shape = $H;
        }

        impl<B, $H, $($T,)* M> SpawnsAs<($H, $($T,)*), There<M>> for B
        where
            B: SpawnsAs<($($T,)*), M>,
        {
            type Shape = <B as SpawnsAs<($($T,)*), M>>::Shape;
        }
    };
}

all_tuples!(impl_spawns_as_tuple, 1, 8, T);

/// The engine's own buffers stay wildcard: any bundle spawns untyped.
impl<B> SpawnsAs<Anything, WildcardMatch> for B {
    type Shape = crate::entity::Untyped;
}

/// Component `Self` may be written incrementally through a facet handle
/// `Entity<H>`.
///
/// Through a real facet only the shape's `Option<T>` parts qualify:
/// required parts exist by construction (strict spawning), so writing one
/// through a facet is either redundant or a shape violation in the making.
/// The untyped handle imposes no shape bound — membership against the
/// `Commands` declaration still applies, with the commit-time conformance
/// check as the runtime backstop.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an `Option<..>` part of shape `{H}`",
    label = "not an optional part of the facet",
    note = "a facet handle `Entity<H>` only accepts the shape's optional parts; required parts are complete at spawn — use an untyped handle (`.untyped()`) for cross-shape writes"
)]
pub trait IncrementOf<H, M> {}

impl<C: Component> IncrementOf<crate::entity::Untyped, ExactMatch> for C {}

impl<C: Component> IncrementOf<Option<C>, Optionally<ExactMatch>> for C {}

macro_rules! impl_increment_of_tuple {
    ($H:ident $(, $T:ident)*) => {
        impl<C, $H, $($T,)* M> IncrementOf<($H, $($T,)*), Here<M>> for C
        where
            C: IncrementOf<$H, M>,
        {
        }

        impl<C, $H, $($T,)* M> IncrementOf<($H, $($T,)*), There<M>> for C
        where
            C: IncrementOf<($($T,)*), M>,
        {
        }
    };
}

all_tuples!(impl_increment_of_tuple, 1, 8, T);

/// One part of a facet, as seen at runtime.
#[derive(Clone, Copy)]
pub struct FacetPart {
    pub type_id: TypeId,
    pub name: &'static str,
    pub optional: bool,
    /// Whether the component participates in revision tracking (untracked
    /// parts contribute no deps, as everywhere else).
    pub tracked: bool,
}

/// A shape element contributing its runtime description to a facet.
pub trait ShapePart {
    fn part() -> FacetPart;
}

impl<C: Component> ShapePart for C {
    fn part() -> FacetPart {
        FacetPart {
            type_id: TypeId::of::<C>(),
            name: std::any::type_name::<C>(),
            optional: false,
            tracked: C::tracked(),
        }
    }
}

impl<T: Component> ShapePart for Option<T> {
    fn part() -> FacetPart {
        FacetPart {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
            optional: true,
            tracked: T::tracked(),
        }
    }
}

/// A facet usable on `Entity<H>`: the untyped handle (no parts, plain
/// identity) or a shape tuple whose required parts drive row matching.
pub trait FacetKind: 'static {
    fn parts() -> Vec<FacetPart>;
}

impl FacetKind for crate::entity::Untyped {
    fn parts() -> Vec<FacetPart> {
        Vec::new()
    }
}

macro_rules! impl_facet_kind {
    ($($P:ident),*) => {
        impl<$($P: ShapePart + 'static,)*> FacetKind for ($($P,)*) {
            fn parts() -> Vec<FacetPart> {
                vec![$($P::part(),)*]
            }
        }
    };
}

all_tuples!(impl_facet_kind, 1, 8, P);

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

/// A bowl-level entity schema: the set of entity *shapes* derived writes
/// are allowed to produce. Implemented by `#[derive(Schema)]` on a
/// named-field struct whose field types are shape tuples; installed at
/// construction via [`BowlBuilder::schema`](crate::BowlBuilder::schema)
/// or a plugin's fragment.
pub trait Schema: 'static {
    fn shapes() -> Vec<ShapeDesc>;
}

/// One named entity shape: the components an entity of this kind carries.
/// `Option<T>` fields in the shape tuple land in `optional`.
///
/// Conformance (checked at commit in debug builds when a schema is
/// registered): each derived write bundle per entity must fit inside one
/// shape, and after the write that shape's required components must all be
/// present on the entity — so a spawn must be complete, while incremental
/// writes may finish a shape another commit started.
pub struct ShapeDesc {
    pub name: &'static str,
    pub required: Vec<(TypeId, &'static str)>,
    pub optional: Vec<(TypeId, &'static str)>,
}

impl ShapeDesc {
    /// Whether `type_id` is part of this shape at all.
    pub(crate) fn contains(&self, type_id: TypeId) -> bool {
        self.required.iter().any(|(id, _)| *id == type_id)
            || self.optional.iter().any(|(id, _)| *id == type_id)
    }
}
