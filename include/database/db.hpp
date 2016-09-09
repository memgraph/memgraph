#pragma once

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

#include "snapshot/snapshot_engine.hpp"
#include "storage/garbage/garbage.hpp"
#include "storage/graph.hpp"
#include "transactions/engine.hpp"

class Indexes;
class Snapshoter;

class Db
{
public:
    using sptr = std::shared_ptr<Db>;

    Db();
    Db(const std::string &name);
    Db(const Db &db) = delete;

    Graph graph;
    tx::Engine tx_engine;
    Garbage garbage;
    SnapshotEngine snap_engine;

    std::string const &name() const;

    Indexes indexes(); // TODO join into Db

    // INDEXES

    // TG - type group
    // I - type of function I:const tx::Transaction& ->
    // std::unique_ptr<IndexBase<TypeGroupVertex,std::nullptr_t>>
    // G - type of collection (verrtex/edge)
    // TODO: Currently only one index at a time can be created.
    // TODO: Move to indexes
    template <class TG, class I, class G>
    bool create_index_on_vertex_property_family(const char *name, G &coll,
                                                I &create_index);

    // Removes index IndexHolder. True if there was index to remove.
    // TODO: Move to indexes
    template <class TG, class K>
    bool remove_index(IndexHolder<TG, K> &ih);

private:
    std::string name_;
};
