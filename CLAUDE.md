# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Porridge is an ECS-inspired **incremental evaluation** prototype for compilers, language tools, and other stateful non-game systems. Facts are components on entities, systems are async functions that derive more facts, and callers query settled results. It is a prototype: the API is still being shaped, and the playground is the main documentation alongside `README.md`, `TODO.md` (roadmap), and `spec/` (design docs — `formal-semantics.md`, `streaming-evaluation.md`, `access-scheduling.md`, etc.).

## Change workflow (mandatory Fable review)

Every change to this repository — code, specs, docs, benchmarks — goes through an external
Fable review session before implementation and again before commit. Read-only work (answering
questions, exploring code) is exempt.

1. **Plan first.** Before touching any file, write a concrete plan (files, approach, tests)
   and send it for review: `fable "<plan>"`. Capture the session id from the response and
   reuse it with `fable --resume "<session>" "<prompt>"` for all follow-ups on the same task;
   start a fresh session only for unrelated work.
2. **Wait for explicit approval.** Ask Fable to answer with exactly `APPROVED PLAN` or
   `CHANGES REQUESTED`. Implement only after a response containing `APPROVED PLAN`. On
   `CHANGES REQUESTED` (or any qualified/ambiguous answer), revise the plan and resubmit to
   the same session — never implement in parallel with an open review.
3. **Implement, then submit for final review.** Send the full diff (`git diff`) plus
   verification evidence — the exact commands run (at minimum `cargo build` and the relevant
   `cargo test`; benches/playground runs when touched) and their pass/fail output — back to
   the *same* session. Ask for exactly `APPROVED` or `CHANGES REQUESTED`. On
   `CHANGES REQUESTED`, apply the requested fixes and resubmit the updated diff and evidence.
4. **Commit gate.** Fable approval is necessary but never sufficient: it does not grant
   commit authorization. The rule under "Commit convention" still applies — the user must
   authorize every commit. Do not commit without both Fable's `APPROVED` and the user's go.

Fable sessions are planning/review only: prompts must instruct Fable not to edit files or
commit, and if a Fable response indicates it modified anything, stop and report it to the
user instead of proceeding.

## Commit convention

Short lowercase imperative subject (e.g. `add guarded component cells`), no body, no attribution/Co-Authored-By trailers. Exception: performance commits may carry a body listing old vs new benchmark numbers.

Do not commit automatically. Only chain commits without per-commit approval when the user has explicitly granted it for a specific set of planned tasks; once that task set is done, the permission expires.

## Commands

```sh
cargo build                          # build workspace
cargo test -p bowl                   # engine tests (all tests live in-crate, mostly in bowl.rs)
cargo test -p bowl <test_name>       # single test
cargo run -p playground              # run the toy-language integration playground
RUST_LOG=info cargo run -p playground  # with tracing output (tracing only initializes if RUST_LOG is set)
cargo run -p playground --release    # release run (used for perf measurement)
cargo bench -p benches               # criterion engine benchmarks (crates/benches/benches/engine.rs)
cargo bench -p benches -- --test     # smoke-test benches without measuring
```

The playground has a `build.rs` that generates parser code from `crates/playground/src/lang/grammar/syntax.llw` via **lelwel**; `grammar/parser.rs` includes generated code, so grammar changes go through the `.llw` file.

## Workspace layout

- `crates/bowl` — the engine (async runtime, storage, queries, systems, scheduling). No test directory; unit tests are inline.
- `crates/benches` — criterion benchmarks for engine slow paths (settle cost, replanning, dense scans, `Where<Eq>` filters, view amplification); fixtures in `src/lib.rs`, benches in `benches/engine.rs`. See `spec/performance-plan.md`.
- `crates/macros` — proc macros. `#[derive(Component)]` attributes: `#[component(hash)]` (implements `fingerprint()` via hashing, enabling revision reuse for equal values), `#[component(revision)]` (fingerprint = the `revision: u64` field verbatim), `#[component(untracked)]` (`tracked() == false`, writes don't bump revisions or invalidate memos), `#[relationship(target = T)]`/`#[relationship_target(relationship = E)]` (engine-maintained inverse). `#[derive(Schema)]` on a named-field struct of shape tuples generates the `Schema` impl plus a companion module of per-shape aliases (`lang_schema::Diagnostic`). `#[derive(SystemParam)]` builds param bundles. The macros emit `::bowl::...` paths directly (no rename support).
- `crates/playground` — toy language built on bowl; serves as the main integration test and usage reference (`cargo test -p playground` runs pipeline consistency tests in `src/tests.rs`). Structured around **language entities** (see `spec/language-entities.md`): `lang/grammar/` is syntax only, `lang/entities/{document,import,definition,namespace}.rs` are vertical slices implementing the stage traits in `lang/entity.rs`, `lang/entities/mod.rs` owns the exhaustive rule→entity lowering dispatch, and `lang/service/` owns request/response facts; `lang/schema.rs` is the single source of truth for entity shapes (`LangSchema`), and the language installs as a plugin (`LangPlugin`). `src/replication.rs` is a dummy replicon-style plugin dogfooding plugin seams and shape-granular replication. Hover uses the **candidate-fact pipeline**: tracked enrichment (`Evaluate`, joins requests to files on `FilePath`) → per-entity candidate systems (`Phase::Complete`, ambient views of lowered facts behind the phase barrier) → arbitration (`Phase::Complete`, a `RequestKey` join yielding one invocation per (request, candidate) pair that monotonically upgrades `HoverRank`/`HoverInfo` — tracked, so same-phase-safe). The namespace entity exercises system query joins (`qualify_members`) and the `#[derive(SystemParam)]` bundle (`QualifiedDefs`).

## Engine architecture (crates/bowl)

The whole model hangs on a few types; understanding their interaction requires reading `bowl.rs`, `world.rs`, `query.rs`, `system.rs`, and `declare.rs` together.

### Construction and schemas (`bowl.rs`, `declare.rs`)

- **Builder-only**: `Bowl::builder().schema::<S>().plugin(P).system(sys).build()` is the only construction path; the system set seals at build (no dynamic registration — conditional subsystems are demand markers gating *planning*). Plugins carry a schema fragment + systems as one unit; the bowl schema is the union of fragments, collected before the presence-bit universe is laid out.
- **Schemas** (`#[derive(Schema)]`): named shapes with `Option<T>` optional parts. Spawns are *strict* — `Commands<S>::insert` must fully match one declared shape (compile-time, `SpawnsAs`) and returns a typed facet handle `Entity<Shape>`. Facet handles bound increments to the shape's optional parts (`IncrementOf`); untyped handles keep membership semantics with commit-time conformance (debug) as backstop. `Commands<S>` has no default and no public wildcard; `Commands<()>` = removal-only writer.
- **Facet queries**: `Query<(Entity<H>, parts…)>` anchors rows to entities conforming to `H` (required set present; presence-mask matched on schema bowls). The anchor contributes *no* revision deps; `Tracked<H>` is the opt-in whole-shape dep part (used by replication capture).
- **Presence bitmaps**: schema bowls assign each universe component a bit at construction; one bitmap per entity, maintained at the world's four mutation chokepoints, copy-on-write against snapshots. Multi-part row matching is one mask check instead of per-part store probing (−96…−98% vs probing; `presence_scan` bench).

### Storage (`world.rs`)

- `World` = `HashMap<TypeId, Store<T>>`; each `Store<T>` is a `BTreeMap<Entity, ComponentEntry<T>>`. There are no archetypes.
- Component values live in shared **guarded cells** (`Arc<ComponentCell<T>>` — a hand-rolled readers/writer lock over `UnsafeCell`). A `Snapshot` is just a structural `World::clone()`: it clones the maps and `Arc`s, not the user data. Live mutation write-locks the cell while snapshots hold read guards.
- Every tracked write bumps a global `Revision` and stamps the entry. If a component has a `fingerprint()` (via `#[component(hash)]`) and the fingerprint is unchanged, the old revision is kept — this is the change-detection cutoff.
- Components have `Origin::Base` (caller-inserted) or `Origin::Derived` with an `owner: SystemInvocation`. Rerunning an invocation first calls `remove_derived_owned(owner)` and then applies its new commands — outputs are *replaced*, not diffed.

### Queries and system params (`query.rs`, `system.rs`)

- `Query<T, F>` is the **tracked** input: each matching row becomes one memoized invocation. Row enumeration drives from the smallest participating store (or the presence mask on schema bowls); only the bare untyped `Entity` param and all-optional rows fall back to the dense `0..next_entity_raw()` scan. Remaining planning work (dirty queues, watermark gating) is TODO §7.
- Per row, a query produces: `keys` (entity ids → invocation identity), `deps` (component `Revision`s → memo invalidation), and `access` (row-level read/write set → conflict scheduling).
- **Joins**: a system query with `Where<Eq<K>>` binds its argument to the unique sibling param reading `&K`; `Where<In<T>>` is the identity join over an engine-maintained relationship inverse. Single-key bound joins are *pair-driven*: rows expand from the already-picked provider's member list / fingerprint bucket during product construction (O(pairs)), and the provider param must precede the bound param. Compound multi-key joins keep product-and-prune. Validated at registration (exactly one provider; `View` rejects bound filters). See `spec/joins.md`.
- `View<T>` is the **ambient** counterpart: same snapshot, but contributes *no* memo deps — a system reruns only when its driving `Query` row changes, even if viewed data changed. This is deliberate (TODO §10) and the duplicate-defs checker relies on it.
- `Commands<S>` buffers writes under a declared output set; they apply only at commit, owned by the emitting invocation. `MutRef<'_, T>` in a system query is a scheduler-visible write edge yielding an in-place `&mut T` (write guard held for the invocation; revision bookkeeping at commit, which also absorbs the row's own write into the memo so systems don't invalidate themselves). `Mut<T>` is external-only (scoop results with `with_original`/`with_latest`). Tuple system params form the cartesian product of their state sets.
- External reads go through `bowl.scoop::<Query<...>>()` (settles first, then materializes from a snapshot), with runtime-argument filters (`Where<Eq<T>>` etc. via `.args(...)`, `Named<Tag, _>` + `.args_for::<Tag>` for per-query args). `Cow<T>` + `for_each` is the clone-on-write external mutation path; `Bowl::insert(...).bind().take::<T>()` is the destructive request/response path (`BoundEntity`).

### Evaluation (`bowl.rs`)

- `Bowl` is `Arc`-cheap to clone. Two locks: `state` (world + memo + generation bookkeeping; held only for short sections) and `runner` (single-flight evaluator; whoever wins `try_lock` drives evaluation, everyone else waits on generation waiters).
- Inputs are batched into **generations**: inserts queue base commands for the next pending generation; `settle()` runs generations until nothing is pending and the revision stops moving, then runs `on_settled` hooks (which may reopen work) and finally the `Settle` phase.
- Phases per generation: `Startup` (first generation only, and after preemption restarts) → `Evaluate` → `Complete`; `Settle` runs at settle time and cannot drive its own settle forward — its removals apply within the settle (reaping before settled reads return) while its inserts/spawns defer to the start of the next run (`State::deferred_settle`).
- `run_phase_streaming` is the core loop: build one `Arc<Snapshot>` + `Arc` memo per wave, plan every system's runnable invocations (`plan_invocations` skips rows whose memoized deps are unchanged), skip rows conflicting with in-flight `Access` sets, poll runs concurrently, then drain all finished invocations and commit them as a batch — a commit whose captured deps went stale is discarded (and marks its row for replanning). The phase replans only when a commit actually changed the world, a run went stale, or a conflict-deferred row was freed.
- Commits replace derived outputs by **diffing**: commands apply over the invocation's previous outputs (equal fingerprints keep their revisions — idempotent reruns cause no invalidation), then stale leftovers are removed via a live-world `derived_owners` index (owner → outputs). That index is deliberately *not* cloned into snapshots (`World`'s manual `Clone`); ownership checks from settled hooks go through `Bowl::has_derived_owned`.
- Each `Store<T>` keeps an `Arc`'d fingerprint → entities index (copy-on-write against snapshots); `Where<Eq<T>>` external filters resolve candidates through it when the argument type is `#[component(hash)]`.
- Memoization: `memo: HashMap<SystemInvocation, MemoEntry>` where `SystemInvocation = system id + query entity keys` and `MemoEntry = Vec<Dep>` (component revisions observed). `CommitLimit` (default 10k commits) is the non-convergence guardrail.
- `DerivedFrom` resolves source-entity revisions at buffer end (deferred through `World::pending_derived_from`, so a later same-buffer write to an anchor cannot make the derived fact stale on arrival); the `cleanup_stale_derived` system (registered by the app in `Phase::Settle`) removes derived entities whose sources changed. `Singleton<T>` markers map a component type to one entity via a world-level index.

### Conventions

- Diagnostics/derived facts should be their own entities tied to sources with `DerivedFrom::new`/`::many`.
- Ephemeral coordination markers are singleton components emitted from `on_settled` hooks and removed by a `Settle`-phase system (removal-only there; a `Startup`-phase retraction twin covers preemption restarts). Markers are for *state*, not ordering — phases and tracked joins carry ordering.
- Schemas are the data model: shapes are defined once (`#[derive(Schema)]`), referenced everywhere (`Commands<(lang_schema::Diagnostic,)>`, `Entity<lang_schema::SourceFile>`), and plugins compose fragments upward.
- `Snapshot` is a type alias for `World`; treat `World` as an implementation detail (it's only public because of `QueryParam`).
