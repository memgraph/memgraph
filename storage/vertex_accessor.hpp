#pragma once

#include "record_accessor.hpp"
#include "vertex.hpp"
#include "vertices.hpp"

class Vertex::Accessor : public RecordAccessor<Vertex, Vertices, Accessor>
{
public:
    using RecordAccessor::RecordAccessor;
};
