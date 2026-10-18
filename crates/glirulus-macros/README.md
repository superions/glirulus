# glirulus-macros

Procedural macros for the [Glirulus](https://github.com/superions/glirulus) durable workflow engine.

## Overview

Provides three macros that eliminate boilerplate when defining workflows:

- **`#[task]`** — Transforms an async function into a `CoreTask` struct with automatic registration, metadata, and dependency injection.
- **`workflow!`** — Builds a workflow pipeline with a concise DSL that desugars to `WorkflowBuilder` method calls.
- **`BranchKey`** — Derives routing keys for fieldless enum variants.

Generated code refers directly to `glirulus_core`; the default workflow codec
also refers directly to `glirulus_runtime`. Declare both packages in the consuming
crate; depending on Macros alone does not make those paths available:

```toml
[dependencies]
glirulus-core = "=1.0.0-glirulus.1"
glirulus-runtime = "=1.0.0-glirulus.1"
```

Runtime's default `macros` feature reexports these macros. The examples below
illustrate the DSL with application-defined types and functions.

## `#[task]`

```rust
use glirulus_runtime::task;

#[task(timeout = "30s", retries = 3, backoff = "100ms")]
async fn charge(order: Order, #[inject] stripe: Arc<Stripe>) -> Result<Receipt, BoxError> {
    stripe.charge(&order).await
}
```

### Attributes

| Attribute | Description |
|---|---|
| `id = "…"` | Override task ID (default: function name) |
| `display_name = "…"` | Human-readable name |
| `description = "…"` | Task description |
| `timeout = "30s"` | Task timeout (`ms`, `s`, `m`, `h` suffixes) |
| `retries = 3` | Maximum retry count |
| `backoff = "100ms"` | Initial retry delay |
| `backoff_multiplier = 2.0` | Exponential multiplier (default: 2.0) |
| `tags = "io"` | Categorization tags (repeatable) |

### Parameters

- Exactly **one** non-`#[inject]` parameter: the task input type
- Zero or more `#[inject]` parameters: dependency-injected fields

### Generated Code

The macro generates a PascalCase struct with a `Task` suffix (e.g., `fn charge` → `struct ChargeTask`) with `new()`, `task_id()`, `metadata()`, `register()`, and a `CoreTask` trait implementation. The original function is preserved for direct use and testing.

## `workflow!`

```rust
use glirulus_runtime::workflow;

let workflow = workflow! {
    name: "order-process",
    steps: [
        validate(order: Order) { validate_order(order) },
        charge,
        (send_email || update_inventory),
        finalize,
    ]
};
```

### Syntax

| Element | Meaning |
|---|---|
| `task_name` | Reference to a `#[task]`-generated struct |
| `name(param: Type) { expr }` | Inline task |
| `(step \|\| step), join` | Parallel fork |
| `delay "5s"` | Durable delay |
| `signal "name"` | Wait for external signal |
| `loop task N` | Loop body task up to N iterations |
| `flow expr` | Inline a child workflow |
| `route key_fn { "k" => [...] }` | Conditional branch |

### Fields

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Workflow ID (string literal) |
| `codec` | No | Codec type path (defaults to `JsonCodec`) |
| `registry` | No | Task registry expression (defaults to `TaskRegistry::new()`) |
| `steps` | Yes | Comma-separated step list inside `[...]` |

## Documentation

Full API docs are available on [docs.rs](https://docs.rs/glirulus-macros).

## License

The original MIT grant and fork attribution are in [LICENSE](LICENSE).
[NOTICE](NOTICE) identifies the Sayiir 1.0.0 source archive.
