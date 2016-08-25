#pragma once

// #include "mvcc/version_list.hpp"
// #include "storage/edge_type/edge_type.hpp"
#include "storage/model/property_model.hpp"
#include "storage/type_group_edge.hpp"

class EdgeType;

class EdgeModel : public PropertyModel<TypeGroupEdge>
{
public:
    // TODO: here should be the reference
    //       but something that is copyable
    //       because this model is copied all the time (mvcc)
    const EdgeType *edge_type{nullptr};
};
