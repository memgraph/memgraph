#pragma once

#include "storage/model/edge_list.hpp"
#include "storage/model/property_model.hpp"

class VertexModel : public PropertyModel
{
public:
    EdgeList out;
    EdgeMap in;
    LabelCollection labels;
};
