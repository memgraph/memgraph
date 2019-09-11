#pragma once

#ifdef MG_SINGLE_NODE_V2
#include "storage/v2/vertex_accessor.hpp"
using VertexAccessor = storage::VertexAccessor;
#endif

#ifdef MG_SINGLE_NODE
#include "storage/single_node/vertex_accessor.hpp"
#endif

#ifdef MG_SINGLE_NODE_HA
#include "storage/single_node_ha/vertex_accessor.hpp"
#endif

// TODO: write documentation for the interface here!
