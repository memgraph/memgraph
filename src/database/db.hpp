#pragma once

#include "storage/graph.hpp"
#include "transactions/engine.hpp"
#include "transactions/commit_log.hpp"

class Db
{
public:
    using sptr = std::shared_ptr<Db>;

    Graph graph;
    tx::Engine tx_engine;

    //  only for test purposes
    std::string identifier()
    {
        return "memgraph";
    }
};
