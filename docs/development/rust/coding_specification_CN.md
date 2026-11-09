# Rust 编码规范

[English](coding_specification.md) · [文档索引](../../index_CN.md)

**状态：** 规范性工程标准。

## 适用范围与规范术语

本规范适用于 SigBot Core 工作区内的 Rust 代码。**必须**表示强制规则，**应当**
表示默认规则，只有记录了充分理由时才可例外，**可以**表示可选实践。源码模块
级约束仍由相应架构文档负责。

## 核心不变量

- 可失败操作必须暴露并保留错误；生产链路不得依赖未经审视的 Panic。
- 每个 Spawn Task 都必须有明确的生命周期、取消和最终错误归属。
- `Arc<T>` 只提供共享所有权。共享可变状态必须由 Lock、Atomic、Channel 或
  Transaction 保护，并覆盖完整不变量。
- 跨 Task 或 Thread 的值必须满足所需的 `Send`、`Sync` 与生命周期约束，不得
  依赖缺乏依据的 `unsafe impl`。
- 编译器保证的内存安全不等于 Order、Wallet、Ledger、Position 或幂等逻辑正确。
- Public Type 与 Module Boundary 应当让非法状态难以表达，并避免 Provider 细节
  泄漏。
- Concurrency、I/O 与 Storage 变更必须按其故障和一致性风险提供相应测试。

## 错误处理

- 可失败操作使用 `Result<T, E>`，并显式传播错误。
- 可复用 Library Error 优先使用 `thiserror`；聚合异构失败的 Application
  Boundary 优先使用 `anyhow`。
- 生产链路避免 `unwrap()` 与 `expect()`。如果 Panic 代表确实不可达的不变量，
  必须说明该不变量为何一定成立。
- 在组件边界添加可操作的错误上下文，但不得包含 Token、Credential、Private
  Key 或未脱敏的敏感 Payload。
- 不得静默丢弃 Task Join Error、Channel Close、Partial Write 或 Transaction
  Failure。

## 所有权、借用与类型设计

- Callee 不需要所有权时，优先借用（`&T`）而不是 Clone。
- 只有独立 Task 或 Thread 确实需要共享所有权时才使用 `Arc<T>`；`Arc` 不会让
  `T` 的内部状态自动具备线程安全性。
- 根据被保护的状态转换选择 `Mutex<T>`、`RwLock<T>`、Atomic、Channel 或存储
  Transaction；不得在未定义不变量的情况下机械地给状态加锁。
- 使用 `Option<T>` 表示缺失值，使用 `UserId`、`OrderId` 等 Newtype 表示领域
  Identity，使用 Enum 代替开放的字符串常量。
- 在组件边界显式转移所有权，并尽量缩小可变状态的 Public Surface。

## 异步运行时与任务所有权

- 项目异步工作使用 Tokio Runtime；除非底层抽象确有需要，否则优先使用
  `async fn`，不要手写 `Future` 实现。
- 只有工作需要独立调度，且 Future 满足 Executor 的 `Send + 'static` 要求时才
  使用 `tokio::spawn`；普通异步组合应当直接 `.await`。
- Spawn 所属组件必须负责取消、关闭顺序和 Join Error；Detached Background
  Work 不得静默失败。
- 不得跨 `.await`、Network I/O、Disk I/O 或其他潜在长耗时操作持有 Lock
  Guard。应先提取所需状态、释放 Guard，再执行操作。
- 当一个 Task 可以独占可变状态时，优先使用消息传递；只有共享内存能更清楚
  表达不变量时才使用共享状态。

### 项目 Clone 命名约定

捕获的共享值需要为子 Task Clone 时，使用 `to_owned()` 和数字后缀，不使用
`_clone` 或 `_for_clone`；嵌套异步 Scope 继续递增后缀：

```rust
let workflow_id0 = workflow_id.to_owned();
tokio::spawn(async move {
    let workflow_id1 = workflow_id0.to_owned();
    process_workflow(workflow_id1).await;
});
```

该约定用于在 Review 时明确捕获值的代际。如果可以建立更简单的所有权边界，
不得仅为绕过 Borrow Checker 而 Clone。

## `Send`、`Sync` 与并发安全

### 语义与自动推导

- `Send` 表示值的所有权可以安全转移到其他 Thread。
- `Sync` 表示 `&T` 可以安全地在线程之间共享；等价地，当 `&T` 是 `Send` 时，
  `T` 是 `Sync`。
- 两者都是 Unsafe Auto Trait。编译器根据类型字段递归推导，因此它们只增加
  编译期约束，不会增加任何运行时同步机制。

常见反例体现了真实限制：

- `Rc<T>` 使用非原子引用计数，既不是 `Send` 也不是 `Sync`。
- `Cell<T>` 与 `RefCell<T>` 允许无同步的内部可变性，因此不是 `Sync`。
- Raw Pointer 不携带足够的所有权、生命周期、别名或同步证据，不能被自动允许
  跨线程使用。

因此，编译器会拒绝在新线程中捕获 `Rc`：

```rust,compile_fail
use std::rc::Rc;

let value = Rc::new(42);
std::thread::spawn(move || {
    println!("{value}");
});
```

### 编译器检查边界

`std::thread::spawn`、多线程 Tokio Task 与线程安全 Channel 会把 `Send` 要求
传播到捕获值或 Message。组合类型继续传播其字段的 Trait Bound。只有 `T`
满足相应的 `Send + Sync` 要求时，`Arc<T>` 才能跨线程使用；原子引用计数不会
修复内部非线程安全状态。

实现需要跨 Task 共享时，应在 Trait Boundary 表达要求：

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

生命周期是另一类约束。一个值可以是 `Send`，但仍包含对
`thread::spawn` 或 `tokio::spawn` 而言存活时间过短的引用。`Send` 负责跨线程
转移安全，`'static` 要求保证捕获的引用不会比其 Owner 活得更久。

### 选择同步边界

同步边界必须覆盖完整业务不变量。例如，检查并扣减 Wallet Balance 是一个
操作，不能拆成两个分别加锁的操作：

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

| 场景                 | 首选机制                               | 必须确认的边界                                   |
| -------------------- | -------------------------------------- | ------------------------------------------------ |
| 跨 Task 共享只读数据 | `Arc<T>`                               | `T` 仍满足线程安全约束                           |
| 少量共享可变状态     | `Arc<Mutex<T>>`                        | 一个 Critical Section 覆盖完整不变量             |
| 读多写少的共享状态   | `Arc<RwLock<T>>`                       | 已评估 Writer Starvation 与 Guard 持有时间       |
| 独立 Counter 或 Flag | `Atomic*`                              | Memory Ordering 与复合操作边界明确               |
| Task 间转移所有权    | Channel                                | 单一 Task 拥有修改，并处理 Close 与 Backpressure |
| 跨实例业务一致性     | Database Transaction + Idempotency Key | 不把进程内 Lock 当成分布式 Transaction           |

### 编译器不能证明的内容

即使所有类型都满足 `Send + Sync`，编译器也不会证明：

- Order 状态转换是否合法；
- Ledger、Balance 与 Position 是否原子提交；
- Message Replay 是否会重复扣款或下单；
- Lock 获取顺序是否可能死锁；
- Atomic Memory Ordering 是否满足算法要求；
- 取消是否可能留下部分 External Effect。

这些保证归属于 State Machine、Transaction Boundary、Idempotency Key、Lock
Order、测试与运行时可观测性。

### `unsafe impl Send/Sync` 策略

绝不能仅为消除编译错误而添加 `unsafe impl Send` 或 `unsafe impl Sync`。只有同时
满足以下条件时才可以考虑：

1. 类型确实使用了编译器无法表达的同步机制；
2. Ownership、Aliasing、Lifetime 与 Memory Ordering 不变量完整记录在实现旁；
3. 所有 Public API 都维持这些不变量；
4. 测试或 Model Checking 覆盖并发交错、失败路径与析构过程；
5. 专项安全审查确认标准同步原语无法表达同一契约。

默认做法是重新设计数据结构，让编译器自动推导这些 Trait。

> Java 对比：Rust 会在编译期拒绝部分不安全共享，Java 则主要依赖显式同步与
> Concurrent Type。两种语言都不能证明业务 Transaction 正确。

## 模块组织

- 遵循工作区物理组件归属，通常为 `src/<component>/src/`；修改跨模块契约前先
  阅读组件架构文档。
- 保持文件专注且高内聚；只有确实属于 Public API 的模块才使用 `pub mod`，
  Provider-specific 细节必须留在所属抽象之后。
- 与所在 Crate 一致地使用 `mod.rs` 或 `<module>.rs`。
- 避免循环依赖，也不得让 Utility Module 聚集无关行为；共享代码必须有明确且
  稳定的 Owner。

## 性能与数据结构

- 按访问模式选择容器：动态 Sequence 使用 `Vec<T>`，固定大小数据使用
  `[T; N]`，Keyed Lookup 使用 `HashMap<K, V>`。
- 调用方通常可以 Borrow、少数路径需要 Ownership 时使用 `Cow<'a, T>`；只有
  Measurement 证明小型内联存储能改善 Hot Path 时才考虑 `SmallVec`。
- Latency-sensitive Trading Path 避免不必要的 Allocation、Clone、Lock
  Contention 与 Serialization。先 Profile 再优化；没有证据时优先保持清晰。
- 持有进程内 Lock 时绝不能执行 Network 或 Disk I/O。

## 测试与验证

- 在 `#[cfg(test)]` Module 中编写聚焦的 Unit Test，异步行为使用
  `#[tokio::test]`。
- 在依赖所属 Boundary Mock External Dependency；Test Double 不得复制其他
  组件的内部实现。
- 覆盖成功、错误、超时、取消、Channel Close 与 Edge Case。
- Concurrency-sensitive 变更应当测试相关并发交错、幂等、Lock Scope、Shutdown
  与析构；普通测试无法提供足够证据时，使用 Stress Test 或 Model Checking。
- Public Contract 变更必须同步更新其架构文档，以及证明当前实现状态的测试。

## 审查清单

- 所有错误是否都被传播或有意处理，且没有泄漏敏感信息？
- 每个 Spawn Task 是否都有明确的取消、关闭与 Join Error 归属？
- 是否可以用消息传递替代共享可变状态，建立更清晰的边界？
- 每个由 `Arc` 持有的可变值是否都有覆盖完整业务不变量的同步机制？
- 是否跨 `.await`、Network I/O 或 Disk I/O 持有 Lock？
- Message Replay 与并发消费是否由稳定 Idempotency Key 保护？
- Trait Object 是否在 Trait 定义处声明必要的 `Send + Sync`？
- 手动 `unsafe impl Send/Sync` 是否满足全部五项准入条件？
- 测试是否覆盖错误路径、并发交错与部分失败？
