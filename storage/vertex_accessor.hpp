#pragma once

#include "storage/vertex.hpp"
#include "storage/record_accessor.hpp"

class Vertices;

class Vertex::Accessor : public RecordAccessor<Vertex, Vertices, Vertex::Accessor>
{
public:
    using RecordAccessor::RecordAccessor;

    size_t out_degree() const
    {
        return this->record->data.out.degree();
    }

    size_t in_degree() const
    {
        return this->record->data.in.degree();
    }
    
    size_t degree()
    {
        return in_degree() + out_degree();
    }
};
