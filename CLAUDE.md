# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Porridge is an ECS-inspired **incremental evaluation** prototype for compilers, language tools, and other stateful non-game systems. Facts are components on entities, systems are async functions that derive more facts, and callers query settled results. It is a prototype: the API is still being shaped, and the playground is the main documentation alongside `README.md`, `TODO.md` (roadmap), and `spec/` (design docs — `formal-semantics.md`, `streaming-evaluation.md`, `access-scheduling.md`, etc.).

## Commit convention

Short lowercase imperative subject (e.g. `add guarded component cells`), no body, no attribution/Co-Authored-By trailers. Exception: performance commits may carry a body listing old vs new benchmark numbers.

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
- `crates/macros` — `#[derive(Component)]` proc macro. Attributes: `#[component(hash)]` (implements `fingerprint()` via hashing, enabling revision reuse for equal values), `#[component(untracked)]` (`tracked() == false`, writes don't bump revisions or invalidate memos). The macro emits `::bowl::...` paths directly (no rename support).
- `crates/playground` — toy language (lexer/parser/AST → diagnostics → hover service) built on bowl; serves as the main integration test and usage reference.

## Engine architecture (crates/bowl)

The whole model hangs on a few types; understanding their interaction requires reading `bowl.rs`, `world.rs`, `query.rs`, and `system.rs` together.

### Storage (`world.rs`)

- `World` = `HashMap<TypeId, Store<T>>`; each `Store<T>` is a `BTreeMap<Entity, ComponentEntry<T>>`. There are no archetypes.
- Component values live in shared **guarded cells** (`Arc<ComponentCell<T>>` — a hand-rolled readers/writer lock over `UnsafeCell`). A `Snapshot` is just a structural `World::clone()`: it clones the maps and `Arc`s, not the user data. Live mutation write-locks the cell while snapshots hold read guards.
- Every tracked write bumps a global `Revision` and stamps the entry. If a component has a `fingerprint()` (via `#[component(hash)]`) and the fingerprint is unchanged, the old revision is kept — this is the change-detection cutoff.
- Components have `Origin::Base` (caller-inserted) or `Origin::Derived` with an `owner: SystemInvocation`. Rerunning an invocation first calls `remove_derived_owned(owner)` and then applies its new commands — outputs are *replaced*, not diffed.

### Queries and system params (`query.rs`, `system.rs`)

- `Query<T, F>` is the **tracked** input: each matching row becomes one memoized invocation. `QueryParam::rows()` currently enumerates `0..next_entity_raw()` and filters by `has::<T>()` — a dense scan over all entity ids ever allocated (this is the known perf hot spot; see TODO §7).
- Per row, a query produces: `keys` (entity ids → invocation identity), `deps` (component `Revision`s → memo invalidation), and `access` (row-level read/write set → conflict scheduling).
- `View<T>` is the **ambient** counterpart: same snapshot, but contributes *no* memo deps — a system reruns only when its driving `Query` row changes, even if viewed data changed. This is deliberate (TODO §10) and the duplicate-defs checker relies on it.
- `Commands` buffers writes; they apply only at commit, owned by the emitting invocation. `Mut<T>` in a system query is a scheduler-visible write edge yielding an inert handle (`with_original`/`with_latest` mutate the live world in a sync closure). Tuple system params form the cartesian product of their state sets.
- External reads go through `bowl.scoop::<Query<...>>()` (settles first, then materializes from a snapshot), with runtime-argument filters (`Where<Eq<T>>` etc. via `.args(...)`, `Named<Tag, _>` + `.args_for::<Tag>` for per-query args). `Cow<T>` + `for_each` is the clone-on-write external mutation path; `Bowl::insert(...).bind().take::<T>()` is the destructive request/response path (`BoundEntity`).

### Evaluation (`bowl.rs`)

- `Bowl` is `Arc`-cheap to clone. Two locks: `state` (world + memo + generation bookkeeping; held only for short sections) and `runner` (single-flight evaluator; whoever wins `try_lock` drives evaluation, everyone else waits on generation waiters).
- Inputs are batched into **generations**: inserts queue base commands for the next pending generation; `settle()` runs generations until nothing is pending and the revision stops moving, then runs `on_settled` hooks (which may reopen work) and finally the `Cleanup` phase.
- Phases per generation: `Startup` (first generation only) → `Evaluate` → `Complete`; `Cleanup` runs at settle time.
- `run_phase_streaming` is the core loop: clone snapshot + memo, plan every system's runnable invocations (`plan_invocations` skips rows whose memoized deps are unchanged), skip rows conflicting with in-flight `Access` sets, poll runs concurrently, and **commit each finished invocation immediately** — a commit whose captured deps went stale is discarded. Any commit that changed the world triggers a full replan of the phase from a fresh snapshot.
- Memoization: `memo: HashMap<SystemInvocation, MemoEntry>` where `SystemInvocation = system id + query entity keys` and `MemoEntry = Vec<Dep>` (component revisions observed). `CommitLimit` (default 10k commits) is the non-convergence guardrail.
- `DerivedFrom` captures source-entity revisions at insert; the `cleanup_stale_derived` system (registered by the app in `Phase::Cleanup`) removes derived entities whose sources changed. `Singleton<T>` markers map a component type to one entity via a world-level index.

### Conventions

- Diagnostics/derived facts should be their own entities tied to sources with `DerivedFrom::new`/`::many`.
- Ephemeral coordination markers (e.g. the playground's `AstAvailable`) are singleton components emitted from `on_settled` hooks and removed by a `Cleanup`-phase system.
- `Snapshot` is a type alias for `World`; treat `World` as an implementation detail (it's only public because of `QueryParam`).
