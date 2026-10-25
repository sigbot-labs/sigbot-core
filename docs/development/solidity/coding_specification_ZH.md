# Solidity 编码规范

[English](coding_specification.md) · [文档索引](../../index_ZH.md)

**适用范围：** 仅在任务范围包含 Solidity 文件时使用。

修改 Solidity 智能合约时，请遵循以下规范：

1. **工具链**：
   - Solidity 开发统一使用 Foundry 工具链
   - 使用 `forge fmt`、`forge build`、`forge test` 与 `forge coverage` 分别执行格式化、编译、测试与覆盖率检查
   - 使用 `anvil` 提供本地链，使用 `cast` 执行 RPC 调用、交易检查与合约交互
   - 使用 `forge install` 和显式 Remapping 管理合约依赖

2. **安全**：
   - 遵循 Consensys 最佳实践
   - 使用 SafeMath 或 Solidity 0.8+ 内置溢出保护
   - 实现访问控制模式
   - 验证所有外部输入
   - 在需要时使用重入保护

3. **Gas 优化**：
   - 使用适当的数据类型（uint256 vs uint8）
   - 高效打包结构体
   - 对历史数据使用事件而不是存储
   - 最小化外部调用

4. **代码组织**：
   - 对可重用代码使用库
   - 为合约交互实现接口
   - 对访问控制使用修饰符
   - 保持合约专注和模块化

5. **测试**：
   - 使用 Foundry 编写全面的测试
   - 测试边界情况和边界条件
   - 使用各种 gas 价格进行测试
   - 对复杂逻辑使用 Forge Fuzz Test 与 Invariant Test

6. **文档**：
   - 对所有函数使用 NatSpec 注释
   - 记录安全考虑
   - 清楚地解释复杂逻辑
   - 包含使用示例
