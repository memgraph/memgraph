#pragma once

#include "storage/graph.hpp"
// #include "transactions/commit_log.hpp"
#include "transactions/engine.hpp"

class Db
{
public:
    using sptr = std::shared_ptr<Db>;

    Db();
    Db(const std::string &name);
    Db(const Db &db) = delete;

    Graph graph;
    tx::Engine tx_engine;

    std::string &name();

private:
    std::string name_;
};
