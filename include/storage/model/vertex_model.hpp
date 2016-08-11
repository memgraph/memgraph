#pragma once

#include "storage/label/label_collection.hpp"
#include "storage/model/edge_list.hpp"
#include "storage/model/property_model.hpp"

class VertexModel : public PropertyModel
{
public:
    EdgeList out;
    EdgeList in;
    LabelCollection labels;
};
