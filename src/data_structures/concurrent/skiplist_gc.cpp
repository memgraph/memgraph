#include "skiplist_gc.hpp"

#include <iostream>
#include "utils/flag_validation.hpp"

DEFINE_VALIDATED_HIDDEN_int32(
    skiplist_gc_interval, 10,
    "Interval of how often does skiplist gc run in seconds. To "
    "disable set to -1.",
    FLAG_IN_RANGE(-1, std::numeric_limits<int32_t>::max()));
