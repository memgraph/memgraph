# Query Memory Tracking Refactor

**Author**
Andreja Tonev (https://github.com/andrejtonev)

**Status**
ACCEPTED

**Date**
2025-07-15

## Problem

The existing query memory tracking system had several critical issues that affected accuracy and reliability:

1. **Jemalloc Hook Limitations**: Query memory tracking was implemented alongside global memory tracking inside jemalloc's hooks. This approach had two fundamental problems:
   - Jemalloc doesn't always explicitly allocate memory when requested, leading to lower tracking results than expected
   - Jemalloc doesn't immediately release memory, causing deallocations to be missed during the brief period when query memory tracking is active

2. **Runtime Module Symbol Issues**: Runtime modules were not using the custom new/delete implementation but instead the one inside libstd, preventing proper memory tracking for user-defined procedures.

## Criteria

The solution was evaluated based on three key metrics:

1. **Memory Tracking Accuracy** (Weight: 50%): The solution must provide precise, real-time memory tracking for queries and procedures
2. **Performance Impact** (Weight: 30%): The solution should maintain or improve performance while fixing the tracking issues
3. **Maintainability** (Weight: 20%): The solution should be robust, well-tested, and easy to maintain

## Decision

### Custom Malloc Implementation

We moved query memory tracking from jemalloc hooks to a custom malloc implementation that tracks allocations and then calls jemalloc. This approach:

- **Eliminates jemalloc timing issues**: By intercepting allocations before they reach jemalloc, we ensure accurate tracking regardless of jemalloc's internal behavior
- **Provides immediate feedback**: Allocations and deallocations are tracked in real-time, not subject to jemalloc's delayed release patterns
- **Enables future features**: This foundation supports user memory tracking and transaction memory limits that were previously problematic

### Dynamic Linking of libstd

We switched from static to dynamic linking of libstd to resolve symbol visibility issues:

- **Exposes necessary symbols**: Dynamic linking makes the custom new/delete operators visible to runtime modules
- **Simplifies symbol management**: Avoids the complexity of manually exposing all required symbols during static linking
- **Maintains compatibility**: Ensures runtime modules use the same memory tracking implementation as the core system

### Implementation Details

The solution introduces:

- **Thread-local query tracker**: A `thread_local` pointer to the active `QueryMemoryTracker` for efficient per-thread tracking
- **ThreadTrackingBlocker**: RAII guard to prevent recursive memory tracking during internal operations
- **Custom malloc wrapper**: Intercepts allocations before jemalloc, ensuring accurate tracking

## Consequences

### Positive Outcomes

1. **Accurate Memory Tracking**: Query and procedure memory usage is now tracked with high precision
2. **Future-Proof Architecture**: The custom malloc implementation enables advanced memory management features
3. **Improved Reliability**: Eliminates race conditions and timing issues that plagued the jemalloc hook approach
4. **Better Debugging**: More predictable memory behavior aids in troubleshooting and performance optimization

### Trade-offs

1. **Build Complexity**: Dynamic linking adds complexity to the build process and deployment
2. **Memory Overhead**: The custom malloc wrapper adds a small performance overhead for all allocations
3. **Maintenance Burden**: Custom memory management requires ongoing maintenance and testing
4. **Platform Dependencies**: The solution may need platform-specific adaptations for different operating systems
