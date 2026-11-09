# 排障

[English](troubleshooting.md) · [部署](deployment_CN.md) · [构建与测试](../development/build-and-test.md)

## 启动、认证与部署

| 现象                         | 检查与处理                                                                                                                                                                                            |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| YAML 无效或配置缺失          | 使用 `sigbot <module> --config <path>`。未知/重复字段、旧别名会失败，程序不隐式加载 `.env`；使用规范 `SIGBOT__...` 覆盖和中央 Secret 引用。                                                           |
| API 拒绝监听、401 或 403     | 按[身份契约](../architecture/platform/api/api_CN.md#iam)检查 Verifier Secret、签名/有效期、Action/Path/Tenant 和领域 Grant；Deny 优先，未签名内部 Header 不能授权。                                   |
| 登录跳转或认证 Web 链路失败  | 核对真实同源 Gateway/Application 域名、TLS、Return URI 和 Authguard 路由；`web-full` 需要真实浏览器 Storage State，签名 Fixture 和公开截图不能代替 Hosted Login。                                     |
| 平台健康但租户组件未启动     | 检查持久不可变 Run、匹配的 `cluster_info`、Worker 就绪/容量、Ownership Label 与 [Controller 前置条件](../architecture/platform/controller/controller_CN.md)；停止单个 Workflow 会保留共享中间件数据。 |
| Pod Pending 或 Worker 不健康 | 检查节点就绪/磁盘压力、Namespace、ServiceAccount、Runtime Secret、Egress 与 PVC；不要移除调度保护。Controller 不创建远端 Secret/Namespace，也不接管外部对象。                                         |
| 镜像拉取失败                 | 核对 `make image-source`、真实 Tag/Digest 与仓库权限。Aliyun 模式拒绝外部镜像，打包/渲染不能证明镜像可拉取；使用既定跳板流程，SSH Host Key 变化须由运维核验。                                         |
| Rust 原生依赖构建失败        | 按[构建与测试](../development/build-and-test.md)准备固定工具链及原生依赖；macOS 修复 Xcode/CLT 选择或在本机指定 SDKROOT，工具链变化后清理旧 Native/CMake 产物。                                       |
| 迁移校验和漂移               | 中止；旧基线数据库需要显式升级/导入方案，不能修改 `mig_version` 绕过检查；迁移使用 Schema Owner，运行账户仅有 DML。                                                                                   |

检查所属作用域日志和私有 Readiness/Metrics。日志携带 Run/Trace，不包含凭据；归档 ACK
只表示本地 SQLite WAL 持久，不表示 S3 上传完成。详见 [Logger 恢复](../architecture/component/logger/logger_CN.md#rust-log-capture)
与 [Messager 补放](../architecture/messager/messager_CN.md)。历史 PASS 总数只证明[当时的执行范围](../development/handover.md#acceptance)。

<a id="cpu"></a>

## CPU 分析

使用配置的私有管理监听，示例为 `localhost:9001`。默认采样 99 Hz、5 秒，保存 Protobuf 后用 pprof 分析：

```bash
curl -X POST -s 'http://localhost:9001/debug/prof/cpu' > /tmp/pprof.out
go tool pprof -top /tmp/pprof.out
```

60 秒火焰图，或 49 Hz、10 秒文本采样：

```bash
curl -X POST -s 'http://localhost:9001/debug/prof/cpu?seconds=60&format=flamegraph' > /tmp/pprof.svg
curl -X POST -s 'http://localhost:9001/debug/prof/cpu?seconds=10&frequency=49&format=text' > /tmp/pprof.txt
```

二进制输出遵循 [pprof Protobuf 格式](https://github.com/google/pprof/blob/master/proto/profile.proto)。

<a id="memory"></a>

## 内存分析

安装 jemalloc 和 [FlameGraph 工具](https://github.com/brendangregg/FlameGraph)，再编译分析 Feature；直接 Cargo 构建前仍需执行常规 Target 大小和资源检查。

```bash
# macOS
brew install jemalloc
# Ubuntu
sudo apt install libjemalloc-dev
curl https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl > /tmp/flamegraph.pl
cargo build --features=profiling-mem-prof
```

为所需模块启用分析；`lg_prof_interval:28` 表示每分配 2^28 字节、约 256 MiB 采样：

```bash
MALLOC_CONF=prof:true,lg_prof_interval:28 ./target/debug/sigbot api --config etc/sigbot.yaml
```

另一个终端捕获 Heap Profile，稍后重复采样，再与基线比较：

```bash
curl -X POST http://localhost:9001/debug/prof/mem > /tmp/sigbot.hprof
jeprof --svg <path_to_sigbot_binary> --base=<baseline_prof> <profile_data> > /tmp/memory.svg
```

Profile 和生成图放在源码目录之外。采集时监控宿主 CPU、可用内存和 I/O，在宿主失稳前
停止本任务所属的低优先级重负载进程；不能删除正式命名生产镜像或无关卷来使测试通过。
