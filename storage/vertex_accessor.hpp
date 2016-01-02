#pragma once

#include "storage/vertex.hpp"
#include "storage/record_accessor.hpp"

class Vertices;

class Vertex::Accessor : public RecordAccessor<Vertex, Vertices, Vertex::Accessor>
{
public:
    using RecordAccessor::RecordAccessor;
};
