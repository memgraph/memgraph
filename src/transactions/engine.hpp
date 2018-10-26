#pragma once

#ifdef MG_SINGLE_NODE
#include "transactions/single_node/engine.hpp"
#endif

#ifdef MG_SINGLE_NODE_HA
#include "transactions/single_node_ha/engine.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "transactions/distributed/engine.hpp"
#endif
