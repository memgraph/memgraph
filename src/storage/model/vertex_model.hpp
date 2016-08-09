#pragma once

#include "edge_list.hpp"
#include "property_model.hpp"
#include "storage/label/label_collection.hpp"
// #include "storage/model/edge_map.hpp"

class VertexModel : public PropertyModel
{
public:
    EdgeList out;
    EdgeList in;
    LabelCollection labels;
};
