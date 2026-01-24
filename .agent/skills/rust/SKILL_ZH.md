---
name: rust
description: Rust 特定的编码标准和最佳实践。适用于所有 Rust 代码文件：**/*.rs
---
您正在使用此项目中的 Rust 代码。遵循这些 Rust 特定指南：

1. **错误处理**：
   - 对可能失败的操作使用 `Result<T, E>`
   - 库错误优先使用 `thiserror`，应用程序错误优先使用 `anyhow`
   - 始终显式处理错误，在生产代码中避免 unwrap()

2. **异步编程**：
   - 对异步操作使用 `tokio` 运行时
   - 优先使用 `async fn` 而不是手动 Future 实现
   - 对并发任务使用 `tokio::spawn`
   - 注意共享状态的 `Send + Sync` 边界
   - 当将 Arc 类型变量传递给子线程时，应使用 `to_owned` 进行克隆，名称应以数字（0、1、2...）结尾，而不是 `_clone` 或 `_for_clone`。例如：如果最初命名为 `workflow_id`，在 `tokio::spawn` 函数内部应命名为 `workflow_id0`。如果其中还有其他异步操作，则应在 `to_owned` 后命名为 `workflow_id1`。

3. **所有权和借用**：
   - 在可能时优先使用借用（`&T`）而不是克隆
   - 对跨线程的共享所有权使用 `Arc<T>`
   - 对内部可变性使用 `Mutex<T>` 或 `RwLock<T>`
   - 考虑对共享可变状态使用 `Arc<Mutex<T>>`

4. **模块组织**：
   - 遵循工作区结构：`src/{module}/src/`
   - 对公共模块使用 `pub mod`
   - 保持模块文件专注和内聚
   - 对模块声明使用 `mod.rs` 或 `{module}.rs`

5. **类型安全**：
   - 对类型安全使用 newtype 模式（例如，`UserId`、`OrderId`）
   - 优先使用枚举而不是字符串常量
   - 对可空值使用 `Option<T>`
   - 利用 Rust 的类型系统进行编译时保证

6. **性能**：
   - 对动态数组使用 `Vec`，对固定大小数组使用 `[T; N]`
   - 对键值查找优先使用 `HashMap`
   - 对借用或拥有的数据使用 `Cow<'a, T>`
   - 考虑对小集合使用 `SmallVec`

7. **测试**：
   - 在 `#[cfg(test)]` 模块中编写单元测试
   - 对异步测试使用 `#[tokio::test]`
   - 适当模拟外部依赖
   - 测试错误情况和边界条件
