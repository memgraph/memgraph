#pragma once

#include "mvcc/version_list.hpp"
#include "property_model.hpp"
#include "storage/edge_type/edge_type.hpp"

class EdgeModel : public PropertyModel
{
public:
    // TODO: here should be the reference
    //       but something that is copyable
    //       because this model is copied all the time (mvcc)
    const EdgeType *edge_type{nullptr};
};
