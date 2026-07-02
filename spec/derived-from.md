# DerivedFrom and Revision-Scoped Derived Facts

`DerivedFrom` marks a fact entity as derived from one or more source entities.

The core use case is stale output cleanup:

```rust
commands.insert((
    DerivedFrom::many([import, system_import_db]),
    Severity::Warning,
    Diagnostic(message),
));
```

Meaning:

```text
this diagnostic was derived from both the import fact and the import database
```

If either source entity changes or is removed, the diagnostic is stale and
`cleanup_stale_derived` should remove it.

## Semantics

When `DerivedFrom` is inserted, the bowl captures the current entity revision of
each source entity:

```text
Diagnostic
  DerivedFrom([
    import @ rev 10,
    system_import_db @ rev 20,
  ])
```

Cleanup later checks every source:

```text
all source revisions match -> keep derived entity
any source revision changed -> remove derived entity
any source entity missing   -> remove derived entity
```

Entity revision is currently coarse:

```text
entity revision = newest tracked component revision on that entity
```

This means changing any tracked component on a source entity invalidates all
derived entities that depend on that source.

## Why Entity-Scoped

The MVP intentionally avoids component-specific anchors:

```rust
DerivedFrom::<ImportDecl>::new(import)
```

That would be more precise, but it makes inference awkward and exposes more of
the revision mechanism in user code.

The current shape is simpler:

```rust
DerivedFrom::new(import)
DerivedFrom::many([import, system_import_db])
```

This works well if entities are cohesive facts:

```text
one import entity
one ast definition entity
one file entity
one request entity
```

## Relationship To Bevy Relationships

This looks related to Bevy-style relationships, but it is not a full
relationship system.

`DerivedFrom::many([a, b])` is a dependency set on the derived entity:

```text
derived entity -> source A
derived entity -> source B
```

A Bevy-like relationship model would likely represent each edge separately and
maintain reverse target collections. Porridge may add that later, but the MVP
keeps `DerivedFrom` as one component because cleanup only needs to answer:

```text
is this derived entity still current?
```

## Patterns

Parse diagnostics:

```text
Diagnostic derived from [FileText entity]
```

Import diagnostics:

```text
Diagnostic derived from [ImportDecl entity, SystemImportDb singleton entity]
```

Duplicate definition diagnostics:

```text
Diagnostic derived from [current AstDef entity, previous AstDef entity]
```

Hover results:

```text
HoverInfo derived from [HoverRequest entity, FileText entity, SymbolIndex entity]
```

## Open Questions

- Should `DerivedFrom` eventually become relationship-backed?
- Do we need component-specific revision anchors for coarse entities?
- Should cleanup be registered manually, through a plugin, or built into bowl?
- Should derived cleanup remove whole entities only, or also support component
  families on durable entities?
- Should stale cleanup explain which source changed?
