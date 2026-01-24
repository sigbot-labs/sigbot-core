---
name: database
description: Database and storage coding guidelines. Related modules: src/core/src/store/**/*.rs (database store implementations: PostgreSQL, MongoDB, SQLite, TimescaleDB), src/types/**/*.rs (type definitions and schemas)
---
You are working with database and storage modules. This includes PostgreSQL, MongoDB, SQLite, and Redis and TimescaleDB(Only for time series data storage related business functions)。

**Key Responsibilities**:
- Database connection management
- Query optimization
- Transaction handling
- Data migration

**Coding Guidelines**:
1. **Connection Management**:
   - Use connection pooling (sqlx, mongodb)
   - Handle connection errors gracefully
   - Implement connection retry logic
   - Monitor connection pool health

2. **Query Optimization**:
   - Use prepared statements for repeated queries
   - Add appropriate database indexes
   - Avoid N+1 query problems
   - Use batch operations when possible

3. **Transactions**:
   - Use transactions for multi-step operations
   - Handle transaction rollbacks correctly
   - Keep transactions short-lived
   - Avoid long-running transactions

4. **Error Handling**:
   - Map database errors to application errors
   - Handle connection failures gracefully
   - Log database errors with context
   - Implement proper retry logic

5. **Migrations**:
   - Version all database migrations
   - Test migrations on sample data
   - Support migration rollbacks
   - Document migration changes

6. **Testing**:
   - Use test databases for integration tests
   - Clean up test data after tests
   - Test transaction behavior
   - Test error scenarios
