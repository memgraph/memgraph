#include "query/engine.hpp"

DEFINE_bool(interpret, true,
            "Use interpretor instead of compiler for query execution.");
DEFINE_string(compile_directory, "./compiled/",
              "Directory in which to write compiled libraries.");
