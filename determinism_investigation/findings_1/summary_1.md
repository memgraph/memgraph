# Stage 1 Summary: Recording

## Recording Details
- **File**: `findings_1/memgraph_session.undo`
- **Size**: 371MB (371,517,996 bytes)
- **BBCount Range**: [1, 58,653,479]
- **Recording Start**: 2025-12-18 19:59:38
- **Recording End**: 2025-12-18 20:14:36
- **Build**: RelWithDebInfo (with debug symbols)

## Test Queries Executed
1. CREATE (a:Person {name: 'Alice', age: 30});
2. CREATE (b:Person {name: 'Bob', age: 25});
3. CREATE (c:Person {name: 'Charlie', age: 35});
4. MATCH + CREATE edge Alice->Bob
5. MATCH + CREATE edge Bob->Charlie
6. MATCH (p:Person) RETURN p.name, p.age;
7. MATCH edges query

## Notes
- Recording is larger than Release build due to debug info
- Memgraph started successfully with all specified flags
- All test queries executed successfully
