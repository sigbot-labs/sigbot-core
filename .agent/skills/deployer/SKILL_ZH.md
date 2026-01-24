---
name: deployer
description: 部署器模块编码指南。相关模块：src/deployer/**/*.rs (部署器服务实现)
---
您正在开发部署器模块 (`SigbotDeployer`)。该微服务管理租户生命周期，并根据数据库中的租户状态自动部署/销毁租户级别的微服务和中间件。

**架构与核心原则**：

部署器模块充当控制器：
- 扫描数据库（PostgreSQL）以获取租户信息（新/已激活/已停用的租户）
- 管理租户级别微服务和中间件的生命周期
- 支持三种部署模式：Kubernetes、Docker 和 Standalone

**核心功能**：

1. **租户扫描**：
   - 定期扫描数据库以查找租户状态变化
   - 检测新租户、已激活租户和已停用租户
   - 使用分布式锁确保一次只有一个实例处理租户
   - 实现分页以高效扫描大型租户列表

2. **组件生命周期管理**：
   - **中间件组件**：EMQX（消息）、PostgreSQL、TimescaleDB
   - **微服务**：数据源接入器、策略运行器、订单管理器、钱包管理器、回测运行器、通知转发器
   - 对于已激活租户：部署所有必需的组件
   - 对于已停用租户：销毁所有已部署的组件

3. **部署模式**：

   **3.1 Kubernetes 模式**：
   - 充当 Kubernetes operator
   - 从数据库读取租户信息（不是 CRD YAML）
   - 使用 `tooling/deploy/helm/**` 中的 Helm charts 部署 sigbot 集群组件
   - 生成并应用租户特定资源的 Kubernetes 清单
   - 管理租户命名空间、部署、服务、configmaps、secrets
   - 使用 kube-rs 或 k8s-openapi 进行 Kubernetes API 交互

   **3.2 Docker 模式**：
   - 使用 `tooling/deploy/compose/*.yaml` 中的 Docker Compose 启动 sigbot 集群
   - 需要访问 Docker daemon（Unix socket 或 TCP 端口 2375）
   - 监控租户状态并启动/停止租户特定的容器
   - 管理租户特定的 docker 网络、卷和容器
   - 使用 bollard 或 docker-rs 进行 Docker API 交互

   **3.3 Standalone 模式**：
   - 在单个进程中运行所有组件
   - 适用于开发和 MVP 演示场景
   - 租户之间没有资源隔离
   - 所有组件在同一进程空间中运行
   - 更简单的调试和开发工作流

**主要职责**：

1. **租户状态监控**：
   - 以可配置的间隔轮询数据库（默认：每 30 秒）
   - 跟踪租户状态变化（status 字段：1 = 已激活，0 = 已停用）
   - 处理租户创建、激活和停用事件

2. **组件部署**：
   - 根据租户属性生成部署配置
   - 为每个租户部署中间件（EMQX、PostgreSQL、TimescaleDB）
   - 部署微服务（datafeed、strategy、order、wallet、backtest、notification）
   - 在 tenant.properties 中存储部署元数据以进行跟踪

3. **组件销毁**：
   - 从 tenant.properties 识别已部署的组件
   - 优雅地关闭并删除租户特定的资源
   - 清理命名空间、容器、网络、卷
   - 更新 tenant.properties 以反映销毁

4. **错误处理**：
   - 优雅地处理部署失败
   - 使用指数退避重试失败的部署
   - 记录所有带有租户上下文的部署操作
   - 保持部署状态一致性

5. **资源管理**：
   - 跟踪每个租户的资源使用
   - 防止资源耗尽
   - 实现资源配额和限制
   - 监控组件健康状态

**编码指南**：

1. **分布式锁定**：
   - 使用分布式锁防止并发租户处理
   - 锁名称：`KUBERNETES_DEPLOYER`、`DOCKER_DEPLOYER`、`STANDALONE_DEPLOYER`
   - 锁超时：10 秒（可配置）
   - 处理完成后释放锁

2. **租户扫描**：
   - 使用分页高效扫描租户
   - 实现安全阈值（默认：每次扫描 1000 个租户）
   - 处理大型租户列表而不会出现内存问题
   - 跟踪最后处理的页面以恢复

3. **组件部署**：
   - 生成唯一资源名称：`{component}-{tenant-id}-{tenant-name}`
   - 使用租户属性存储部署元数据
   - 在所有部署操作中包含租户上下文
   - 支持从 tenant.properties 获取组件特定配置

4. **Kubernetes 集成**：
   - 使用 kube-rs 或 k8s-openapi 进行 Kubernetes API
   - 从 Helm 模板或直接 YAML 生成清单
   - 创建租户特定的命名空间
   - 将组件部署为 Kubernetes Deployments/StatefulSets
   - 管理用于租户配置的 ConfigMaps 和 Secrets
   - 使用 Service 对象进行组件发现

5. **Docker 集成**：
   - 使用 bollard 或 docker-rs 进行 Docker API
   - 通过 Unix socket (`/var/run/docker.sock`) 或 TCP (`localhost:2375`) 连接
   - 创建租户特定的 Docker 网络
   - 管理容器生命周期（创建、启动、停止、删除）
   - 处理持久数据的卷挂载
   - 对复杂的多容器部署使用 Docker Compose

6. **Standalone 模式**：
   - 在进程中初始化所有组件
   - 使用共享数据库连接
   - 无资源隔离（所有租户共享相同资源）
   - 更简单的启动/关闭逻辑
   - 仅适用于开发

7. **配置**：
   - 扫描间隔的 Cron 表达式（默认：`0/30 * * * * *`）
   - 并发处理的通道大小（默认：5）
   - 分页的安全阈值（默认：1000）
   - 组件特定的超时和重试策略

8. **错误处理**：
   - 记录所有带有租户上下文的错误
   - 实现临时失败的重试逻辑
   - 处理部分部署失败
   - 保持部署状态一致性
   - 向监控系统报告部署状态

9. **测试**：
   - 测试租户扫描逻辑
   - 测试组件部署/销毁
   - 测试错误场景（部署失败、网络问题）
   - 测试分布式锁定行为
   - 测试失败时的资源清理
   - 在单元测试中模拟 Kubernetes/Docker API

10. **安全**：
    - 永远不要记录敏感的租户信息
    - 保护 Docker/Kubernetes API 访问
    - 对 Kubernetes 操作使用 RBAC
    - 在部署前验证租户属性
    - 清理资源名称以防止注入攻击

11. **性能**：
    - 并发处理租户（在限制内）
    - 使用连接池进行数据库访问
    - 缓存频繁访问的租户信息
    - 在可能时批量操作
    - 监控部署操作延迟

12. **可观测性**：
    - 记录所有带有租户上下文的部署操作
    - 跟踪部署指标（成功/失败率、延迟）
    - 在部署后监控组件健康状态
    - 在部署失败时发出警报
    - 跟踪每个租户的资源使用

**组件部署详情**：

1. **中间件组件**：
   - **EMQX**：用于消息传递的 MQTT 代理
   - **PostgreSQL**：主数据库
   - **TimescaleDB**：时序数据库扩展

2. **微服务**：
   - **数据源接入器**：从交易所摄取市场数据
   - **策略运行器**：执行交易策略
   - **订单管理器**：管理订单生命周期
   - **钱包管理器**：管理钱包操作
   - **回测运行器**：运行回测场景
   - **通知转发器**：转发通知

**租户属性模式**：

在 `tenant.properties` JSON 中存储部署元数据：
```json
{
  "deployments": {
    "kubernetes": {
      "namespace": "sigbot-tenant-{id}",
      "components": {
        "emqx": { "deployment": "emqx-{id}", "status": "running" },
        "postgresql": { "deployment": "postgresql-{id}", "status": "running" },
        "datafeed": { "deployment": "datafeed-{id}", "status": "running" }
      }
    },
    "docker": {
      "network": "sigbot-tenant-{id}",
      "containers": {
        "emqx": { "container": "emqx-{id}", "status": "running" }
      }
    }
  }
}
```

**实现注意事项**：

- 对所有 I/O 操作使用 async/await
- 实现适当的错误处理和恢复
- 支持优雅关闭
- 使用带有租户上下文的结构化日志
- 遵循项目中的现有代码模式
- 确保共享状态的线程安全
- 对共享可变状态使用 Arc<Mutex<>>
- 在错误时实现适当的资源清理
