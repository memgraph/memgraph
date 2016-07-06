#pragma once

#include "property_model.hpp"
#include "storage/label/label_collection.hpp"
#include "edge_list.hpp"

class VertexModel : public PropertyModel
{
public:
    EdgeList in, out;
    LabelCollection labels;
};
