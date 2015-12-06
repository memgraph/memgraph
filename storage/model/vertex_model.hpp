#pragma once

#include "property_model.hpp"
#include "label_list.hpp"
#include "edge_list.hpp"

class VertexModel : public PropertyModel
{
public:
    EdgeList in, out;
    LabelList labels;
};
