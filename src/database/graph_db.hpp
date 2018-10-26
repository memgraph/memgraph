#pragma once

#ifdef MG_SINGLE_NODE
#include "database/single_node/graph_db.hpp"
#endif

#ifdef MG_SINGLE_NODE_HA
#include "database/single_node_ha/graph_db.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "database/distributed/graph_db.hpp"
#endif
