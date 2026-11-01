# SigBot Core 文档

[English index](index.md)

本文是现行架构、开发规范、运维 Runbook、用例和历史记录的唯一入口。每个设计
概念只有一个规范归属，其他文档通过链接引用，不重复维护同一契约。

## 架构

[系统总览](architecture/overview_ZH.md)是跨组件边界、受约束的类型化量化交易 Workflow、Service/
Runtime Scope、内嵌 SDK、命名、数据所有权及全部 Engine 设计链接的唯一目录与规范
归属。详细设计仍按物理组件放在 `architecture/engine/`。

## 用例

| 用例 | English | 中文 | 范围 |
|---|---|---|---|
| Researcher 集成 | [规范](use-cases/researcher.md) | [规范](use-cases/researcher_ZH.md) | 与外部 `sigbot-researcher` 项目的边界 |

## 开发生命周期

稳定的语言和框架规范统一放在对应技术子目录，英文规范文件使用
`*_specification.md` 命名。

| 技术或主题 | English | 中文 |
|---|---|---|
| Rust | [编码规范](development/rust/coding_specification.md) | [编码规范](development/rust/coding_specification_ZH.md) |
| Solidity | [编码规范](development/solidity/coding_specification.md) | [编码规范](development/solidity/coding_specification_ZH.md) |
| Python Strategy SDK | [SDK 规范](development/python/strategy-sdk_specification.md) | [SDK 规范](development/python/strategy-sdk_specification_ZH.md) |

编码代理当前实现、证据和已知缺口的交接入口是
[HARDCOVER](development/HARDCOVER.md)。它是运维交接快照，不替代权威架构文档。

高频变更的 Plan、Draft、Experiment 和实施记录统一放在
`development/in-progress/`，冻结的工程记录统一放在
`development/archive/`。两者都不是当前产品行为的权威依据。

| 生命周期 | 项目 | 含义 |
|---|---|---|
| 进行中 | [Copy Trading 策略研究](development/in-progress/copytrading/README_ZH.md) | 已登记的研究原型；其中脚本不是生产源码 |
| 归档 | [历史记录](development/archive/index.md) | 冻结的草案、审计和实施快照 |

## 运维 Runbook

| 范畴 | 文档 |
|---|---|
| 本地环境 | [开发前置条件](runbooks/development-prerequisites.md) |
| 构建测试 | [构建与测试](runbooks/build-and-test.md) |
| 部署 | [Docker](runbooks/build-and-deploy-docker.md) · [Kubernetes](runbooks/deploy-kubernetes.md) |
| 性能分析 | [CPU](runbooks/profile-cpu.md) · [内存](runbooks/profile-memory.md) |

## 语言约定

- 现行设计与指南以不带语言后缀的英文文件为规范源。
- 中文版本与英文源文件同目录，使用 `_ZH.md` 后缀。
- 历史记录可以保持原始单语形态，避免重写历史材料。
- 现行双语文档发生语义变更时，必须在同一变更中同步更新。

## 维护规则

- 每个设计概念只能有一个规范归属；其他文档通过链接引用，禁止复制。
- Engine 架构按物理组件组织；跨组件目录只在 Architecture Overview 中维护，禁止
  在本索引重复。
- 必须区分已实现、部分实现、保留设计和已知限制。
- 可执行配置和生产源码留在所属源码目录，文档只建立链接。
- 禁止向文档树加入空分类、生成缓存、密钥或运行时产物。
- 文档变更完成前必须校验相对链接、标题和现行文档的双语配对。
