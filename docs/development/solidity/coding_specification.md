# Solidity Coding Specification

[中文](coding_specification_ZH.md) · [Documentation index](../../index.md)

**Scope:** Apply only when Solidity files exist in the task scope.

Follow these guidelines when changing Solidity smart contracts:

1. **Toolchain**:
   - Foundry is the required Solidity development toolchain
   - Use `forge fmt`, `forge build`, `forge test`, and `forge coverage` for formatting, compilation, testing, and coverage
   - Use `anvil` for local chains and `cast` for RPC calls, transaction inspection, and contract interaction
   - Manage contract dependencies with `forge install` and explicit remappings

2. **Security**:
   - Follow Consensys Best Practices
   - Use SafeMath or Solidity 0.8+ built-in overflow protection
   - Implement access control patterns
   - Validate all external inputs
   - Use reentrancy guards where needed

3. **Gas Optimization**:
   - Use appropriate data types (uint256 vs uint8)
   - Pack structs efficiently
   - Use events instead of storage for historical data
   - Minimize external calls

4. **Code Organization**:
   - Use libraries for reusable code
   - Implement interfaces for contract interactions
   - Use modifiers for access control
   - Keep contracts focused and modular

5. **Testing**:
   - Write comprehensive tests with Foundry
   - Test edge cases and boundary conditions
   - Test with various gas prices
   - Use Forge fuzz and invariant tests for complex logic

6. **Documentation**:
   - Use NatSpec comments for all functions
   - Document security considerations
   - Explain complex logic clearly
   - Include usage examples
