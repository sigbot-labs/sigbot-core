---
name: solidity
description: Solidity smart contract coding standards. Applies to all Solidity files: **/*.sol
---
You are working with Solidity smart contracts. Follow these Solidity-specific guidelines:

1. **Security**:
   - Follow Consensys Best Practices
   - Use SafeMath or Solidity 0.8+ built-in overflow protection
   - Implement access control patterns
   - Validate all external inputs
   - Use reentrancy guards where needed

2. **Gas Optimization**:
   - Use appropriate data types (uint256 vs uint8)
   - Pack structs efficiently
   - Use events instead of storage for historical data
   - Minimize external calls

3. **Code Organization**:
   - Use libraries for reusable code
   - Implement interfaces for contract interactions
   - Use modifiers for access control
   - Keep contracts focused and modular

4. **Testing**:
   - Write comprehensive tests with Hardhat or Foundry
   - Test edge cases and boundary conditions
   - Test with various gas prices
   - Use fuzzing for complex logic

5. **Documentation**:
   - Use NatSpec comments for all functions
   - Document security considerations
   - Explain complex logic clearly
   - Include usage examples
