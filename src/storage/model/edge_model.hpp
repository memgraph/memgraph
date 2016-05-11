#pragma once

#include "property_model.hpp"
#include "edge_type.hpp"
#include "mvcc/version_list.hpp"

class EdgeModel : public PropertyModel
{
public:
    VertexRecord* from;
    VertexRecord* to;

    EdgeType edge_type;
};
