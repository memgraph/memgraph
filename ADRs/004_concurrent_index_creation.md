# Concurrent Index Creation ADR

**Author**
Gareth Lloyd (https://github.com/Ignition), Ivan Milinović

**Status**
ACCEPTED

**Date**
April 23, 2025

**Problem**

Previously, index creation was a unique access operation, meaning that:
- Queries cannot run concurrently while the index is being made
- This blocks all operations until index creation completes
- Poor user experience during large dataset index creation

**Criteria**

- Allow index population (most expensive part) to happen concurrent with other queries
- Allow read-only queries to be uninterrupted during index creation
- Planning can only use indexes once they are fully populated and ready
- Maintain ACID guarantees and consistency during concurrent operations
- Support cancellation of long-running index creation operations

**Decision**

## Three-Phase Index Creation

We implemented a three-phase approach to index creation that allows concurrent operations:

### Phase 1: Register (READ_ONLY access)
- Create index metadata and reserve the index structure
- Requires READ_ONLY access to prevent writers being unaware of the index to write enries into
- Quick operation that sets up the IndexStatus with `kTransactionInitialId` (populating state)
- Index is not yet available for query planning

### Phase 2: Populate (downgrade to READ access)
- Downgrade access level from READ_ONLY to READ to allow concurrent writers
- Iterate through all existing vertices and populate the index
- New transactions can continue inserting vertices while population is in progress
- Uses MVCC timestamp isolation to ensure consistent population
- Population can be cancelled via `CheckCancelFunction`
- Concurrent insertions by other transactions are handled via skip list thread-safety

### Phase 3: Publish (atomic commit)
- Atomically update the IndexStatus with the commit timestamp
- Index becomes visible to query planner once `IsVisible(timestamp)` returns true
- Plan cache invalidation is triggered only when index is ready for use
- Index is now available for query optimization

## Key Implementation Components

### IndexStatus (`src/storage/v2/inmemory/indices_mvcc.hpp:20`)
```cpp
struct IndexStatus {
  bool IsPopulating() const; // commit_timestamp == kTransactionInitialId
  bool IsReady() const;      // !IsPopulating()
  bool IsVisible(uint64_t timestamp) const; // commit_timestamp <= timestamp
  void Commit(uint64_t timestamp);  // atomic publish
}
```

### ActiveIndices System (`src/storage/v2/indices/active_indices.hpp:29`)
- Tracks which indices are ready for query planning
- `CheckIndicesAreReady()` prevents using unpopulated indices
- Integrates with query planner to ensure only ready indices are used

### Used Index Checker (`src/query/plan/used_index_checker.hpp:19`)
- Validates that all required indices for a query plan are ready
- Integrated into query planning pipeline
- Prevents execution of plans that depend on still-populating indices

### Concurrent Skip List Architecture
- Skip lists are inherently thread-safe for concurrent reads/writes
- MVCC validation ensures transaction isolation during index scans
- Population uses snapshot isolation for consistency
- New entries from concurrent transactions are automatically included

## Benefits Achieved

1. **Non-blocking Reads**: Read-only queries continue uninterrupted during index creation
2. **Concurrent Writes**: Write transactions can proceed during population phase
3. **Cancellable Operations**: Long-running index creation can be terminated
4. **Plan Cache Efficiency**: Cache invalidation only occurs when index is ready
5. **MVCC Consistency**: Full transaction isolation maintained throughout process

## Storage Interface Changes

Enhanced the Storage interface with new parameters:
- `CheckCancelFunction` for cancellation support
- `PublishIndexWrapper` for custom publication logic
- `DropIndexWrapper` for symmetric drop operations

Example signature change:
```cpp
// Before
virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
    LabelId label, std::vector<storage::PropertyPath> properties) = 0;

// After
virtual utils::BasicResult<StorageIndexDefinitionError, void> CreateIndex(
    LabelId label, PropertiesPaths properties,
    CheckCancelFunction cancel_check = neverCancel,
    PublishIndexWrapper wrapper = publish_no_wrap) = 0;
```

## Error Handling

New error types in `storage_error.hpp:25`:
- `IndexDefinitionCancelationError` for cancelled operations
- Proper error propagation through all three phases

## Testing

Comprehensive testing added in:
- `tests/e2e/concurrency/` - End-to-end concurrent operation tests
- `tests/unit/storage_v2_indices.cpp` - Unit tests for concurrent index operations
- Integration with existing index test suites

## Implementation Details

### Query Context Updates (`src/query/context.hpp:67`)
- Introduced `StoppingContext` to replace individual transaction status tracking
- Consolidated cancellation logic across query execution and index operations

### Plan Validation
- Query plans are validated against active indices before execution
- Plan cache invalidation occurs only when indices transition to ready state
- Prevents using indices that are still populating

### MVCC Integration (`src/storage/v2/inmemory/indices_mvcc.hpp`)
- Population respects transaction timestamp boundaries
- Concurrent updates are properly isolated using MVCC
- Index visibility tied to transaction commit timestamps

## Current Implementation Status

### ✅ **Implemented**
- **Main Database**: Full three-phase concurrent index creation for skiplist based indexes:
  - Label indices
  - Label+property indices
  - Edge-type indices
  - Edge-type+property indices
  - Edge global property indices
- **Single Transaction**: Register/Populate/Publish all occur within one transaction
- **Plan Integration**: Query planner correctly ignores unpopulated indices
- **Cancellation**: Robust cleanup of partially populated indices
- **MVCC Isolation**: Proper snapshot isolation during population

### 🚧 **Limitations & Future Work**

#### **Replica Replication** (src/dbms/inmemory/replication_handlers.cpp:777-1336)
```cpp
// TODO: add when concurrent index creation can actually replicate using READ_ONLY
// constexpr auto kReadOnlyAccess = storage::Storage::Accessor::Type::READ_ONLY;

// Multiple TODO comments throughout:
// TODO: For now kUniqueAccess, when everything is ready kReadOnlyAccess
auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
```
- **Current**: All replica operations use `kUniqueAccess` (blocking)
- **Reason**: Replication delta ordering needs to be resolved first
- **Impact**: Replicas don't benefit from concurrent index creation yet

#### **TTL Multi-Index Operations**
- **Current**: TTL operations don't use Register/Populate/Publish pattern yet
- **Issue**: Can only downgrade access once per transaction
- **Workaround**: TTL uses `kUniqueAccess` to avoid multiple downgrades
- **Future**: Split TTL into separate Register→(downgrade)→Populate/Publish phases

#### **Auto-indexing** ✅ (Implemented as of July 2025)
- **Implemented**: Auto-indexing now runs in dedicated background thread with separate transactions
- **Architecture**: `AutoIndexStructure` with thread-safe skiplist queue manages index creation requests
- **Transaction Handling**: Requests queued during user transaction, processed asynchronously post-commit
- **Concurrency**: Read-only transactions with 500ms timeout, automatic retry on failure
- **Implementation Details**:
  - Dedicated `auto_index_thread_` processes queued requests
  - Uses `index_auto_creation_queue_` (skiplist) for thread-safe request management
  - Condition variable `index_auto_creation_cv_` for efficient wake-up
  - Supports both `LabelId` and `EdgeTypeId` via `std::variant`
- **Benefits**: Zero blocking of user transactions, isolated failures, automatic retry mechanism

## Architecture Decisions Made

### **ActiveIndices Snapshot**
- **Decision**: Transactions capture `ActiveIndices` at start, newer transactions see updates via `UpdateOnX` methods
- **Rationale**: Ensures no writes are missed during population while allowing concurrent access
- **Implementation**: `transaction.active_indices_` provides transaction-local view

### **Access Level Progression**
- **Decision**: READ_ONLY → READ rather than UNIQUE throughout
- **Rationale**: Allows concurrent writes during expensive population phase
- **Constraint**: Only one downgrade per transaction (affects TTL)

## Future Improvements

1. **Replica READ_ONLY Support**: Resolve delta ordering to enable `kReadOnlyAccess` in replication
2. **TTL Register/Populate/Publish Split**: Enable TTL to register multiple indices then downgrade once

## Impact

This implementation significantly improves the user experience for large datasets by:
- Reduce blocking operations and their impact during index creation
- Maintaining query performance during index population
- Providing graceful cancellation of long-running operations
- Ensuring consistency through proper MVCC integration
