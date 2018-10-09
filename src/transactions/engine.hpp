#pragma once

#ifdef MG_SINGLE_NODE
#include "transactions/single_node/engine.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "transactions/distributed/engine.hpp"
#endif
