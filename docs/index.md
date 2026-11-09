# SigBot Core documentation

[中文](index_CN.md)

Start with the [architecture overview](architecture/overview.md) for the typed
trading Workflow, service boundaries, Provider lifecycle, configuration, data
ownership and implementation limits. Each design has one owning document.

## Architecture

| Area                                           | English                                                      | 中文                                                            |
| ---------------------------------------------- | ------------------------------------------------------------ | --------------------------------------------------------------- |
| System                                         | [Overview](architecture/overview.md)                         | [总览](architecture/overview_CN.md)                             |
| Platform API: HTTP, CRUD, IAM and external MCP | [API](architecture/platform/api/api.md)                      | [API](architecture/platform/api/api_CN.md)                      |
| Agent interoperability                         | [A2A](architecture/platform/api/a2a.md)                      | [A2A](architecture/platform/api/a2a_CN.md)                      |
| Platform scheduling                            | [Controller](architecture/platform/controller/controller.md) | [Controller](architecture/platform/controller/controller_CN.md) |
| Nine tenant components                         | [Component catalog](architecture/overview.md#core-services)  | [组件目录](architecture/overview_CN.md#核心服务)                |
| Embedded messaging SDK                         | [Messager](architecture/messager/messager.md)                | [Messager](architecture/messager/messager_CN.md)                |

Component designs live at `architecture/component/<module>/<module>.md`, matching
`src/cmd/src/startup/component/`: backtest, datafeed, evaluator, exporter, logger,
notifier, order, strategy and wallet. The embedded Exchange SDK is owned by the
[Order document](architecture/component/order/order.md#exchange-sdk); active
market snapshots by Datafeed; Rust log capture by Logger.

## Development

| Topic                                 | English                                                               | 中文                                                            |
| ------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------- |
| Rust                                  | [Coding specification](development/rust/coding_specification.md)      | [编码规范](development/rust/coding_specification_CN.md)         |
| Solidity                              | [Coding specification](development/solidity/coding_specification.md)  | [编码规范](development/solidity/coding_specification_CN.md)     |
| Python Strategy SDK                   | [SDK specification](development/python/strategy-sdk_specification.md) | [SDK 规范](development/python/strategy-sdk_specification_CN.md) |
| Build, setup and verification         | [Build and test](development/build-and-test.md)                       | —                                                               |
| Handover, evidence and remaining work | —                                                                     | [Development handover](development/handover.md)                 |

Development drafts and handovers belong in `development/`. Keep only active,
useful material and merge settled design into its architectural owner. Test
reports belong in their case's timestamped reports, not duplicated in `docs/`.

## Operations

- Deployment prerequisites and Docker/Kubernetes/Helm commands: [English](runbooks/deployment.md) · [中文](runbooks/deployment_CN.md)
- Troubleshooting and CPU/memory profiling: [English](runbooks/troubleshooting.md) · [中文](runbooks/troubleshooting_CN.md)

English documents and `_CN.md` companions live together and change together.
Distinguish normative design, implemented behavior, reserved capabilities and
actual acceptance evidence. Link to the owner instead of creating another
migration note, summary or duplicated contract. Keep temporary programs,
generated output and obsolete implementation records out of this tree.
