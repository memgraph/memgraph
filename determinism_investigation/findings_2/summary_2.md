# Stage 2 Summary: Event Statistics

## Recording Info
- **Total Events**: ~134,857 (approximate from counts)
- **BBCount Range**: [1, 58,653,479]

## Top Non-Determinism Sources (by count)

| Rank | Event Type | Count | % | Notes |
|------|------------|-------|---|-------|
| 1 | THREADSWITCH | 27,844 | 20.65% | Thread scheduling |
| 2 | clock_gettime | 25,295 | 18.76% | Time operations - **HIGH** |
| 3 | futex | 20,580 | 15.26% | Thread synchronization |
| 4 | WALLCLOCK_TIME | 18,990 | 14.08% | Wall clock captures |
| 5 | newfstatat | 15,616 | 11.58% | File stat operations |
| 6 | gettimeofday | 7,605 | 5.64% | Time operations |
| 7 | access | 3,555 | 2.64% | File access checks |
| 8 | gettid | 2,134 | 1.58% | Thread ID lookups |
| 9 | read | 1,549 | 1.15% | File reads |
| 10 | getrandom | 17 | 0.01% | RNG - **very low** |

## Key Insights

### 1. Time Operations Dominate
Combined time-related events:
- clock_gettime: 25,295
- WALLCLOCK_TIME: 18,990
- gettimeofday: 7,605
- time: 271
- **Total: 52,161 events (~38.7%)**

### 2. Thread Operations
Combined thread-related events:
- THREADSWITCH: 27,844
- futex: 20,580
- gettid: 2,134
- clone3: 127
- NEWTHREAD: 128
- **Total: 50,813 events (~37.7%)**

### 3. File System Operations
- newfstatat: 15,616
- access: 3,555
- read: 1,549
- **Total: ~20,720 events (~15.4%)**

### 4. Random Number Generation
- getrandom: Only 17 calls!
- This means RNG is seeded once at startup and PRNGs are used thereafter
- **Not a significant source of non-determinism at runtime**

## Categorized by Reducibility

### Potentially Reducible (Investigate Further)
1. **clock_gettime** (25,295) - Investigate call sources
2. **gettimeofday** (7,605) - Investigate call sources
3. **WALLCLOCK_TIME** (18,990) - Side effect of time syscalls

### Likely Necessary but Review
1. **THREADSWITCH** (27,844) - Review thread model
2. **futex** (20,580) - Lock contention analysis
3. **newfstatat** (15,616) - Why so many file stats?

### Unavoidable
1. **NEWTHREAD** (128) - Thread creation at startup
2. **getrandom** (17) - One-time seeding

## Next Steps
- Stage 3: Investigate clock_gettime and gettimeofday call stacks
- Stage 4: Investigate getrandom sources (low priority)
- Stage 5: Investigate thread switching patterns
