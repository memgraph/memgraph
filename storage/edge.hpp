#pragma once

#include "model/properties/jsonwriter.hpp"
#include "model/edge_model.hpp"
#include "mvcc/record.hpp"

class Edge;

class Edge : public mvcc::Record<Edge>
{
public:
    Edge() = default;
    Edge(const EdgeModel& data) : data(data) {}
    Edge(EdgeModel&& data) : data(std::move(data)) {}

    Edge(const Edge&) = delete;
    Edge(Edge&&) = delete;

    Edge& operator=(const Edge&) = delete;
    Edge& operator=(Edge&&) = delete;

    EdgeModel data;
};
