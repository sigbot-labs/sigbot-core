# Cursor 配置说明

本目录包含 Cursor IDE 的 AI 助手配置，包括全局提示词和模块化规则。

## 文件结构

```
.cursor/
├── prompt              # 全局提示词（唯一，始终应用）
└── rules/              # 模块化规则（多个，按需应用）
    ├── global.mdc      # 全局规则（alwaysApply: true）
    ├── rust.mdc        # Rust 语言规范
    ├── api.mdc         # API 模块规范
    ├── datafeed.mdc    # 数据源模块规范
    └── ...             # 其他模块规范
```

## `.cursor/prompt` - 全局提示词

### 特点
- ✅ **唯一性**：只能有一个 `prompt` 文件
- ✅ **始终应用**：每次 AI 对话都会包含此内容
- ✅ **全局上下文**：提供项目整体背景和 AI 角色定义

### 用途
- 定义 AI 助手的角色和基本行为
- 提供项目整体架构和背景信息
- 设置全局编码偏好和原则

### 格式
纯文本文件，无需 YAML frontmatter

### 示例
```text
You are a senior Rust AI programming assistant working on SigBot...

**Project Context**:
- SigBot is a high-performance trading bot...

**Your Role**:
- Provide expert coding assistance...
```

## `.cursor/rules/*.mdc` - 模块化规则

### 特点
- ✅ **多个文件**：可以为每个模块创建独立的规则文件
- ✅ **按需应用**：根据 `globs` 模式匹配文件路径
- ✅ **详细规范**：提供具体的技术实现指导

### 用途
- 定义特定模块的详细编码规范
- 提供技术实现指导和最佳实践
- 包含具体的功能需求和架构要求

### 格式
Markdown 文件 + YAML frontmatter

### 示例
```markdown
---
description: Order management module coding guidelines
globs: src/order/**/*.rs,src/core/src/modules/order/**/*.rs
alwaysApply: false
---
You are working on the Order Manager module...
```

## 与 Claude Code Skills 的区别

| 特性 | Cursor Rules | Claude Code Skills |
|------|-------------|-------------------|
| 格式 | `.mdc` 文件 | `SKILL.md` + 资源文件 |
| 作用域 | 项目级别 | 功能模块级别 |
| 应用方式 | 自动应用（基于 globs） | 按需调用 |
| 可执行代码 | ❌ | ✅ |
| 跨项目复用 | ❌ | ✅ |

Cursor Rules 更适合项目内部的编码规范，而 Claude Code Skills 更适合可复用的功能扩展。

## 两者的关系和优先级

### 应用顺序（从高到低）

1. **`.cursor/prompt`** - 全局提示词
   - 始终应用
   - 提供项目整体上下文

2. **`.cursor/rules/global.mdc`** - 全局规则
   - `alwaysApply: true` 时始终应用
   - 提供项目级别的通用规范

3. **`.cursor/rules/{module}.mdc`** - 模块规则
   - 当编辑的文件匹配 `globs` 时应用
   - 提供模块特定的详细规范

### 叠加效果

```
编辑 src/order/server/order_server.rs 时：

┌─────────────────────────────────────┐
│ .cursor/prompt                       │ ← 始终应用
│ (全局提示词)                         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ .cursor/rules/global.mdc            │ ← 始终应用
│ (全局规则)                           │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ .cursor/rules/rust.mdc               │ ← 匹配 *.rs
│ (Rust 语言规范)                      │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ .cursor/rules/order.mdc             │ ← 匹配 src/order/**/*.rs
│ (订单模块规范)                       │
└──────────────────────────────────────┘
```

## 最佳实践

### 1. `.cursor/prompt` 应该包含：
- ✅ AI 角色定义
- ✅ 项目整体背景和架构
- ✅ 全局编码原则（安全、性能等）
- ❌ 避免过于详细的技术规范（放在 rules 中）

### 2. `.cursor/rules/global.mdc` 应该包含：
- ✅ 项目级别的通用编码规范
- ✅ 跨模块的最佳实践
- ✅ 语言特定的通用规则

### 3. `.cursor/rules/{module}.mdc` 应该包含：
- ✅ 模块特定的详细规范
- ✅ 功能需求和架构要求
- ✅ 具体的技术实现指导

### 4. 避免重复：
- ❌ 不要在 `prompt` 和 `global.mdc` 中重复相同内容
- ✅ `prompt` 提供上下文，`rules` 提供规范

## 配置建议

### 当前配置

- **`.cursor/prompt`**: 项目背景 + AI 角色 + 全局原则
- **`.cursor/rules/global.mdc`**: 详细的全局编码规范（`alwaysApply: true`）
- **`.cursor/rules/{module}.mdc`**: 各模块的详细规范

### 推荐分工

| 文件 | 内容 | 长度 |
|------|------|------|
| `prompt` | 项目背景、AI 角色、核心原则 | 简短（~10-20 行） |
| `global.mdc` | 通用编码规范、最佳实践 | 中等（~30-50 行） |
| `{module}.mdc` | 模块详细规范、功能需求 | 详细（~50-150 行） |

## 常见问题

### Q: 可以创建多个 `prompt` 文件吗？
A: ❌ 不可以。Cursor 只支持一个 `.cursor/prompt` 文件。

### Q: `prompt` 和 `global.mdc` 有什么区别？
A: 
- `prompt`: 全局提示词，提供项目背景和 AI 角色
- `global.mdc`: 全局规则，提供详细的编码规范（可以设置 `alwaysApply: true`）

### Q: 如果内容重复怎么办？
A: 建议：
- `prompt`: 保持简短，只包含项目背景和核心原则
- `global.mdc`: 包含详细的全局编码规范
- 避免在两个文件中重复相同内容

### Q: 规则文件的优先级是什么？
A: 所有匹配的规则文件会**叠加应用**，没有覆盖关系。更具体的规则（匹配更多文件路径的）会提供更详细的指导。

