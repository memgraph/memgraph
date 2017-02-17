#pragma once

#if __GNUC__ >= 3
// make a hint for the branch predictor in a conditional or a loop
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif
