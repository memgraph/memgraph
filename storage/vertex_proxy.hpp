#pragma once

#include "record_proxy.hpp"

class Vertices;

class VertexProxy : public RecordProxy<Vertex, Vertices, VertexProxy>
{
    using RecordProxy::RecordProxy;
};
