#pragma once

#include <vector>

#include "mvcc/version_list.hpp"
#include "storage/edge_record.hpp"

class EdgeList
{
public:
    auto begin() { return edges.begin(); }
    auto begin() const { return edges.begin(); }
    auto cbegin() const { return edges.begin(); }

    auto end() { return edges.end(); }
    auto end() const { return edges.end(); }
    auto cend() const { return edges.end(); }

    void add(EdgeRecord *edge) { edges.emplace_back(edge); }

    size_t degree() const { return edges.size(); }

    void remove(EdgeRecord *edge)
    {
        edges.erase(std::remove(edges.begin(), edges.end(), edge), edges.end());
    }

    void clear() { edges.clear(); }

    std::size_t size() { return edges.size(); }

private:
    std::vector<EdgeRecord *> edges;
};
