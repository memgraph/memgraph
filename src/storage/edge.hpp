#pragma once

#include "mvcc/record.hpp"
#include "model/edge_model.hpp"
#include "model/properties/traversers/jsonwriter.hpp"

class Edge : public mvcc::Record<Edge>
{
public:
    class Accessor;

    Edge() = default;
    Edge(const EdgeModel& data) : data(data) {}
    Edge(EdgeModel&& data) : data(std::move(data)) {}

    Edge(const Edge&) = delete;
    Edge(Edge&&) = delete;

    Edge& operator=(const Edge&) = delete;
    Edge& operator=(Edge&&) = delete;

    EdgeModel data;
};
