# SigBot Configuration Architecture Design

## Architecture Overview

SigBot 采用分层架构，分为：
- **Platform 级 Services**: apiserver, deployer 等平台级服务
- **租户级 Services**: strategy runner, backtest runner, datafeed ingestor 等租户级微服务

## Problem Statement

1. **配置共享问题**: `src/core/src/config/config.rs` 中的 `AppConfig` 是全局共享的配置结构
2. **动态部署需求**: 当 deployer 为新租户激活时，会动态部署租户级中间件（Postgres、Redis 等），需要设置租户特定的连接信息（host、port、username、password 等）
3. **配置修改需求**: 租户级微服务启动时需要根据租户配置动态修改全局配置中的中间件连接信息

## Solution Design

### Core Components

1. **CustomConfigConfigurer Trait** (`config.rs`)
   - 定义配置修改器接口，允许根据自定义配置 JSON 修改 `AppConfigProperties`
   - 实现者可以提取租户配置中的组件信息并应用到基础配置

2. **DefaultTenantConfigConfigurer** (`config_tenant.rs`)
   - 默认租户配置修改器实现
   - 从租户配置 JSON 中提取组件信息（Postgres、Redis、TimescaleDB）
   - 更新数据库连接、Redis 连接、向量数据库连接等配置

3. **Initialization Pattern**
   - `init_tenant_config(configuration: &str)`: 启动时调用一次，初始化租户配置
   - `get_tenant_config()`: 无参数，返回已缓存的租户配置，可在服务内部任意地方调用

### Usage Pattern

```rust
// 在租户级服务启动时（如 strategy runner）
let configuration = matches.get_one::<String>("STRATEGY_RUNNER_CONFIGURATION").unwrap();
init_tenant_config(configuration);  // 初始化一次

// 之后在服务内部任意地方
let config = get_tenant_config();  // 无参数调用
let db_host = &config.appdb.postgres.inner.host;
```

### Key Design Principles

1. **启动时初始化**: 租户配置在服务启动时初始化一次，避免重复解析
2. **全局访问**: 初始化后可在服务内部任意位置无参数调用 `get_tenant_config()`
3. **向后兼容**: 如果未初始化，`get_tenant_config()` 返回平台级配置
4. **代码分离**: 租户配置相关实现放在 `config_tenant.rs`，保持 `config.rs` 简洁

## Implementation Files

- `src/core/src/config/config.rs`: 核心配置结构和 `CustomConfigConfigurer` trait
- `src/core/src/config/config_tenant.rs`: 租户配置实现和 `DefaultTenantConfigConfigurer`
- `src/strategy/runner/src/server/strategy_runner.rs`: 使用示例

## Future Improvements

- 支持密码解密（当前密码加密存储，需要租户私钥解密）
- 支持更多中间件类型配置
- 支持配置热更新
