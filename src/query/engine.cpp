#include "query/engine.hpp"

DEFINE_bool(INTERPRET, true,
            "Use interpretor instead of compiler for query execution.");
DEFINE_string(COMPILE_DIRECTORY, "./compiled/",
              "Directory in which to write compiled libraries.");
