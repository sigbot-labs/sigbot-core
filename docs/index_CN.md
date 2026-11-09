# SigBot Core 文档

[English](index.md)

从[架构总览](architecture/overview_CN.md)开始，了解类型化交易 Workflow、服务边界、
Provider 生命周期、配置、数据所有权与实现限制。每项设计只有一份归属文档。

## 架构

| 范畴                                | English                                                      | 中文                                                            |
| ----------------------------------- | ------------------------------------------------------------ | --------------------------------------------------------------- |
| 系统                                | [Overview](architecture/overview.md)                         | [总览](architecture/overview_CN.md)                             |
| 平台 API：HTTP、CRUD、IAM、外部 MCP | [API](architecture/platform/api/api.md)                      | [API](architecture/platform/api/api_CN.md)                      |
| Agent 互操作                        | [A2A](architecture/platform/api/a2a.md)                      | [A2A](architecture/platform/api/a2a_CN.md)                      |
| 平台调度                            | [Controller](architecture/platform/controller/controller.md) | [Controller](architecture/platform/controller/controller_CN.md) |
| 九个租户组件                        | [Component catalog](architecture/overview.md#core-services)  | [组件目录](architecture/overview_CN.md#核心服务)                |
| 内嵌消息 SDK                        | [Messager](architecture/messager/messager.md)                | [Messager](architecture/messager/messager_CN.md)                |

组件设计统一为 `architecture/component/<module>/<module>.md`，与
`src/cmd/src/startup/component/` 对应：backtest、datafeed、evaluator、exporter、logger、
notifier、order、strategy、wallet。内嵌 Exchange SDK 归入 [Order](architecture/component/order/order_CN.md#exchange-sdk)，
主动行情快照归 Datafeed，Rust 日志采集归 Logger。

## 开发

| 主题                | English                                                               | 中文                                                            |
| ------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------- |
| Rust                | [Coding specification](development/rust/coding_specification.md)      | [编码规范](development/rust/coding_specification_CN.md)         |
| Solidity            | [Coding specification](development/solidity/coding_specification.md)  | [编码规范](development/solidity/coding_specification_CN.md)     |
| Python Strategy SDK | [SDK specification](development/python/strategy-sdk_specification.md) | [SDK 规范](development/python/strategy-sdk_specification_CN.md) |
| 交接、证据与待办    | —                                                                     | [开发交接](development/handover.md)                             |

开发草案与 Handover 统一归 `development/`，只保留仍有用的内容；已稳定设计合并到所属
架构文档。执行报告留在对应 Case 的时间戳报告目录，不在 `docs/` 重复堆放。

开发环境与验证步骤：[构建与测试](development/build-and-test.md)。

## 操作手册

- 部署依赖与 Docker/Kubernetes/Helm 命令：[English](runbooks/deployment.md) · [中文](runbooks/deployment_CN.md)
- 排障与 CPU/内存分析：[English](runbooks/troubleshooting.md) · [中文](runbooks/troubleshooting_CN.md)

英文与 `_CN.md` 中文文档同目录、同步维护。明确区分设计规范、已实现行为、预留能力和
实际验收证据。其他文档链接唯一归属，不再另建迁移说明、阶段总结或重复契约；临时脚本、
生成产物和失效实施记录不进入文档树。
