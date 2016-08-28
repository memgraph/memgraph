#pragma once

#include "communication/bolt/v1/states/state.hpp"
#include "communication/bolt/v1/session.hpp"
#include "query_engine/query_engine.hpp"

namespace bolt
{

class Executor : public State
{
    struct Query
    {
        std::string statement;
    };

public:
    Executor();

    State* run(Session& session) override final;

protected:
    Logger logger;

    /* Execute an incoming query
     *
     */
    void run(Session& session, Query& query);

    /* Send all remaining results to the client
     *
     */
    void pull_all(Session& session);

    /* Discard all remaining results
     *
     */
    void discard_all(Session& session);

private:
    QueryEngine<communication::OutputStream> query_engine;

};

}

