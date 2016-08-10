#pragma once

#include "storage/graph.hpp"
#include "transactions/engine.hpp"
#include "transactions/commit_log.hpp"

class Db
{
public:
    using sptr = std::shared_ptr<Db>;

    Db() = default;
    Db(const std::string& name) : name_(name) {}
    Db(const Db& db) = delete;

    Graph graph;
    tx::Engine tx_engine;

    std::string& name()
    {
        return name_;
    }

private:
    std::string name_;
};
