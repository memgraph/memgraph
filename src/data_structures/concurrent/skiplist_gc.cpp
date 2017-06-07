#include "skiplist_gc.hpp"
#include "gflags/gflags.h"

DEFINE_int32(skiplist_gc_interval, 10,
             "Interval of how often does skiplist gc run in seconds. To "
             "disable set to 0.");
