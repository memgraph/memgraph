#pragma once

#ifdef MG_SINGLE_NODE
#include "storage/single_node/edge_accessor.hpp"
#endif

#ifdef MG_SINGLE_NODE_HA
#include "storage/single_node_ha/edge_accessor.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "storage/distributed/edge_accessor.hpp"
#endif

// TODO: write documentation for the interface here!
