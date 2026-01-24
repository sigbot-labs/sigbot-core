---
name: strategy-sdk-python
description: Python code standards for strategy SDK. Applies to all Python files: **/*.py
---
You are working with Python code in the SigBot strategy SDK. Follow these Python-specific guidelines:

1. **Code Style**:
   - Follow PEP 8 style guide
   - Use type hints for function signatures
   - Use descriptive variable names
   - Keep functions focused and small

2. **Strategy SDK**:
   - Use the provided SDK functions for market data access
   - Implement proper error handling
   - Use async/await for I/O operations
   - Document all strategy functions

3. **Performance**:
   - Use efficient data structures (numpy arrays for numerical data)
   - Avoid unnecessary data copying
   - Use generators for large datasets
   - Profile performance-critical code

4. **Testing**:
   - Write unit tests for strategy logic
   - Test edge cases and error conditions
   - Use pytest for testing framework
   - Mock external dependencies

5. **Documentation**:
   - Use docstrings for all functions and classes
   - Follow Google or NumPy docstring style
   - Include examples in docstrings
   - Document strategy parameters clearly
