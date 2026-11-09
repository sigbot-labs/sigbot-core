# Rust Coding Specification

[中文](coding_specification_CN.md) · [Documentation index](../../index.md)

**Status:** Normative engineering specification.

## Scope and Normative Terms

This specification applies to Rust code in the SigBot Core workspace. **MUST**
marks a required rule, **SHOULD** marks the default unless a documented reason
justifies an exception, and **MAY** marks an optional practice. Source-level
module constraints remain owned by the corresponding architecture document.

## Core Invariants

- Fallible operations MUST expose and preserve errors; production paths MUST
  not depend on an unexamined panic.
- Every spawned task MUST have an owner for its lifetime, cancellation, and
  terminal error.
- `Arc<T>` provides shared ownership only. Shared mutation MUST be protected by
  a lock, atomic operation, channel, or transaction that covers the complete
  invariant.
- Values crossing task or thread boundaries MUST satisfy the required `Send`,
  `Sync`, and lifetime bounds without unsupported `unsafe impl` declarations.
- Compiler-checked memory safety does not prove order, wallet, ledger,
  position, or idempotency correctness.
- Public types and module boundaries SHOULD make invalid states difficult to
  represent and provider details difficult to leak.
- Concurrency, I/O, and storage changes MUST receive tests proportionate to
  their failure and consistency risk.

## Error Handling

- Use `Result<T, E>` for fallible operations and propagate errors explicitly.
- Prefer `thiserror` for reusable library error types and `anyhow` at
  application boundaries that aggregate heterogeneous failures.
- Avoid `unwrap()` and `expect()` in production paths. If a panic represents a
  genuinely unreachable invariant, document why the invariant is guaranteed.
- Add actionable context at component boundaries, but never include tokens,
  credentials, private keys, or unredacted sensitive payloads.
- Do not silently discard task join errors, channel closure, partial writes, or
  transaction failures.

## Ownership, Borrowing, and Type Design

- Prefer borrowing (`&T`) over cloning when the callee does not need ownership.
- Use `Arc<T>` only when ownership must be shared across independently owned
  tasks or threads. An `Arc` does not make `T` internally thread-safe.
- Use `Mutex<T>`, `RwLock<T>`, atomics, channels, or storage transactions
  according to the state transition being protected; do not wrap state in a
  lock without defining its invariant.
- Use `Option<T>` for absence, newtypes such as `UserId` and `OrderId` for
  domain identity, and enums instead of open-ended string constants.
- Keep ownership transfer explicit at component boundaries and minimize the
  public surface of mutable state.

## Async Runtime and Task Ownership

- Use the Tokio runtime for project asynchronous work and prefer `async fn`
  over manual `Future` implementations unless a low-level abstraction requires
  one.
- Use `tokio::spawn` only for work that needs independent scheduling and whose
  future satisfies the executor's `Send + 'static` requirements. Ordinary
  async composition SHOULD use direct `.await`.
- The spawning component MUST own cancellation, shutdown ordering, and join
  error handling. Detached background work MUST not fail silently.
- Do not hold a lock guard across `.await`, network I/O, disk I/O, or other
  potentially long operations. Extract the required state, release the guard,
  and then perform the operation.
- Prefer message passing when one task can own the mutable state. Use shared
  memory only when it expresses the invariant more clearly.

### Project Clone Naming Convention

When a captured shared value must be cloned for a child task, use `to_owned()`
and a numeric suffix rather than `_clone` or `_for_clone`. Increment the suffix
for nested async scopes:

```rust
let workflow_id0 = workflow_id.to_owned();
tokio::spawn(async move {
    let workflow_id1 = workflow_id0.to_owned();
    process_workflow(workflow_id1).await;
});
```

This convention makes capture generations visible during reviews. Do not
clone merely to satisfy the borrow checker when a simpler ownership boundary
is available.

## `Send`, `Sync`, and Concurrency Safety

### Semantics and Automatic Derivation

- `Send` means ownership of a value can be transferred safely to another
  thread.
- `Sync` means `&T` can be shared safely between threads; equivalently, `T` is
  `Sync` when `&T` is `Send`.
- Both are unsafe auto traits. The compiler derives them recursively from a
  type's fields, so they add compile-time constraints without adding a runtime
  synchronization mechanism.

Common counterexamples encode real restrictions:

- `Rc<T>` uses non-atomic reference counts and is neither `Send` nor `Sync`.
- `Cell<T>` and `RefCell<T>` permit unsynchronized interior mutation and are
  not `Sync`.
- Raw pointers do not carry enough ownership, lifetime, aliasing, or
  synchronization evidence to cross thread boundaries automatically.

The compiler therefore rejects an `Rc` captured by a new thread:

```rust,compile_fail
use std::rc::Rc;

let value = Rc::new(42);
std::thread::spawn(move || {
    println!("{value}");
});
```

### Enforcement Boundaries

`std::thread::spawn`, multi-thread Tokio tasks, and thread-safe channels
propagate `Send` requirements to captured values or messages. Composite types
propagate the bounds of their fields. `Arc<T>` can cross threads only when `T`
satisfies the corresponding `Send + Sync` requirements; its atomic reference
count does not repair unsafe interior state.

Express requirements at a trait boundary when implementations will be shared
across tasks:

```rust
use std::sync::Arc;

trait StrategyInfoHandler: Send + Sync {
    fn handle(&self, input: &str);
}

fn share_handler(handler: Arc<dyn StrategyInfoHandler>) {
    let handler0 = handler.to_owned();
    std::thread::spawn(move || handler0.handle("market-event"));
}
```

Lifetimes are a separate constraint. A value can be `Send` and still contain a
reference that is too short-lived for `thread::spawn` or `tokio::spawn`.
`Send` governs transfer safety; `'static` requirements ensure captured
references cannot outlive their owners.

### Choosing a Synchronization Boundary

The synchronization boundary MUST cover the complete business invariant. For
example, checking and debiting a wallet balance are one operation, not two
independently locked operations:

```rust,ignore
use std::sync::Mutex;

struct Wallet {
    balance: Mutex<i64>,
}

impl Wallet {
    fn debit(&self, amount: i64) -> Result<bool, WalletError> {
        let mut balance = self.balance.lock().map_err(WalletError::from)?;
        if *balance < amount {
            return Ok(false);
        }
        *balance -= amount;
        Ok(true)
    }
}
```

| Scenario                            | Preferred mechanism                    | Required reasoning                                              |
| ----------------------------------- | -------------------------------------- | --------------------------------------------------------------- |
| Shared immutable data               | `Arc<T>`                               | `T` still satisfies thread-safety bounds                        |
| Small shared mutable state          | `Arc<Mutex<T>>`                        | One critical section covers the invariant                       |
| Read-heavy shared state             | `Arc<RwLock<T>>`                       | Writer starvation and guard duration are evaluated              |
| Independent counter or flag         | `Atomic*`                              | Memory ordering and compound operations are explicit            |
| Ownership transfer between tasks    | Channel                                | One task owns mutation and handles closure/backpressure         |
| Cross-instance business consistency | Database transaction + idempotency key | Process-local locks are not treated as distributed transactions |

### What the Compiler Cannot Prove

Even when every type is `Send + Sync`, the compiler does not prove:

- that order state transitions are legal;
- that ledger, balance, and position changes commit atomically;
- that message replay cannot debit or submit twice;
- that lock acquisition order cannot deadlock;
- that an atomic memory ordering is sufficient for the algorithm;
- that cancellation cannot leave partial external effects.

State machines, transaction boundaries, idempotency keys, lock-order rules,
tests, and runtime observability own those guarantees.

### `unsafe impl Send/Sync` Policy

Never add `unsafe impl Send` or `unsafe impl Sync` merely to eliminate a
compiler error. It may be considered only when all of the following hold:

1. The type uses a synchronization mechanism the compiler cannot represent.
2. Ownership, aliasing, lifetime, and memory-ordering invariants are fully
   documented next to the implementation.
3. Every public API preserves those invariants.
4. Tests or model checking cover interleavings, failure paths, and destruction.
5. A focused safety review confirms that standard synchronization primitives
   cannot express the same contract.

The default is to redesign the data structure so the compiler can derive the
traits automatically.

> Java note: Rust rejects some unsafe sharing at compile time, while Java
> relies primarily on explicit synchronization and concurrent types. Neither
> language proves business transaction correctness.

## Module Organization

- Follow the workspace's physical component ownership, typically
  `src/<component>/src/`, and read the component architecture document before
  changing cross-module contracts.
- Keep files focused and cohesive; use `pub mod` only for intentional public
  modules and keep provider-specific details behind the owning abstraction.
- Use `mod.rs` or `<module>.rs` consistently with the surrounding crate.
- Avoid circular dependencies and utility modules that collect unrelated
  behavior. Shared code MUST have a clear stable owner.

## Performance and Data Structures

- Select containers by access pattern: `Vec<T>` for dynamic sequences,
  `[T; N]` for fixed-size data, and `HashMap<K, V>` for keyed lookup.
- Use `Cow<'a, T>` when callers can usually borrow but some paths require
  ownership. Consider `SmallVec` only when measurements show that small inline
  storage benefits a hot path.
- Avoid unnecessary allocation, cloning, lock contention, and serialization on
  latency-sensitive trading paths. Optimize after profiling and preserve
  clarity unless evidence justifies complexity.
- Never perform network or disk I/O while holding a process-local lock.

## Testing and Verification

- Place focused unit tests in `#[cfg(test)]` modules and use `#[tokio::test]`
  for asynchronous behavior.
- Mock external dependencies at the owning boundary; do not reproduce another
  component's implementation in a test double.
- Cover success, error, timeout, cancellation, channel-closure, and edge cases.
- Concurrency-sensitive changes SHOULD test relevant interleavings,
  idempotency, lock scope, shutdown, and destruction. Use stress tests or model
  checking when ordinary tests cannot provide enough evidence.
- A change to a public contract MUST update its architecture document and the
  tests that establish the implemented status.

## Review Checklist

- Are all errors propagated or intentionally handled without leaking secrets?
- Does every spawned task have explicit cancellation, shutdown, and join-error
  ownership?
- Could message passing replace shared mutation with a clearer boundary?
- Does each `Arc`-owned mutable value have a synchronization mechanism that
  covers the complete business invariant?
- Is any lock held across `.await`, network I/O, or disk I/O?
- Are replay and concurrent consumption protected by a stable idempotency key?
- Do trait objects declare the necessary `Send + Sync` bounds at the trait?
- Does any manual `unsafe impl Send/Sync` satisfy all five policy conditions?
- Do tests cover error paths, concurrent interleavings, and partial failure?
