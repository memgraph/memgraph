#pragma once

#ifdef MG_SINGLE_NODE
#include "database/single_node/graph_db_accessor.hpp"
#endif

#ifdef MG_SINGLE_NODE_HA
#include "database/single_node_ha/graph_db_accessor.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "database/distributed/graph_db_accessor.hpp"
#endif
