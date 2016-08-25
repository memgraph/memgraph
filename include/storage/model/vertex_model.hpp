#pragma once

#include "storage/label/label_collection.hpp"
#include "storage/model/edge_list.hpp"
#include "storage/model/edge_map.hpp"
#include "storage/model/property_model.hpp"

class VertexModel : public PropertyModel<TypeGroupVertex>
{
public:
    EdgeList out;
    EdgeMap in;
    LabelCollection labels;
};
