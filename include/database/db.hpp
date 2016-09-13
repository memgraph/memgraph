#pragma once

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

#include "snapshot/snapshot_engine.hpp"
#include "storage/garbage/garbage.hpp"
#include "storage/graph.hpp"
#include "transactions/engine.hpp"

class Indexes;

// Main class which represents Database concept in code.
// TODO: Maybe split this in another layer between Db and Dbms. Where the new
// layer would hold SnapshotEngine and his kind of concept objects. Some
// guidelines would be: retain objects which are necessary to implement querys
// in Db, the rest can be moved to the new layer.
class Db
{
public:
    using sptr = std::shared_ptr<Db>;

    explicit Db(bool import_snapshot = true);
    Db(const char *name, bool import_snapshot = true);
    Db(const std::string &name, bool import_snapshot = true);
    Db(const Db &db) = delete;

private:
    const std::string name_;

public:
    tx::Engine tx_engine;
    Graph graph;
    Garbage garbage = {tx_engine};

    // This must be initialized after name.
    SnapshotEngine snap_engine = {*this};

    // Creates Indexes for this db.
    // TODO: Indexes should be created only once somwhere Like Db or layer
    // between Db and Dbms.
    Indexes indexes();

    std::string const &name() const;
};
