#pragma once

#ifdef MG_SINGLE_NODE
#include "storage/single_node/vertex_accessor.hpp"
#endif

#ifdef MG_DISTRIBUTED
#include "storage/distributed/vertex_accessor.hpp"
#endif

// TODO: write documentation for the interface here!
