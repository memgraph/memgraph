#pragma once

#include <vector>

#include "query_engine/code_generator/cypher_state.hpp"
#include "query_engine/code_generator/query_action.hpp"
#include "query_engine/exceptions/exceptions.hpp"

class CppGenerator
{
public:
    // !! multithread problem
    // two threads shouldn't use this implementation at the same time
    // !! TODO: REFACTOR

    CppGenerator() : unprocessed_index(0), processed_index(0) { setup(); }

    void state(CypherState state) { _cypher_state = state; }

    CypherState state() { return _cypher_state; }

    std::string generate()
    {
        std::string code = "";

        for (uint64_t i = processed_index; i < unprocessed_index; ++i) {
            auto &action = actions.at(i);
            auto query_action = action.first;
            if (action_functions.find(query_action) == action_functions.end())
                throw CppGeneratorException(
                    "Query Action Function is not defined");
            auto &action_data = action.second;
            code += action_functions[query_action](_cypher_data, action_data);
            ++processed_index;
        }

        return code;
    }

    QueryActionData &add_action(const QueryAction &query_action)
    {
        unprocessed_index++;
        actions.push_back(std::make_pair(query_action, QueryActionData()));
        return action_data();
    }

    QueryActionData &action_data() { return actions.back().second; }

    CypherStateData &cypher_data() { return _cypher_data; }

    void clear()
    {
        processed_index = 0;
        unprocessed_index = 0;
        actions.clear();
    }

private:
    // TODO: setup function is going to be called every time
    // when object of this class is constructed (optimize this)
    void setup()
    {
        action_functions[QueryAction::TransactionBegin] =
            transaction_begin_action;
        action_functions[QueryAction::Create] = create_query_action;
        action_functions[QueryAction::Match] = match_query_action;
        action_functions[QueryAction::Return] = return_query_action;
        action_functions[QueryAction::Set] = set_query_action;
        action_functions[QueryAction::TransactionCommit] =
            transaction_commit_action;
    }

    uint64_t unprocessed_index;
    uint64_t processed_index;
    std::vector<std::pair<QueryAction, QueryActionData>> actions;
    std::map<QueryAction,
             std::function<std::string(CypherStateData &cypher_data,
                                       QueryActionData &action_data)>>
        action_functions;
    CypherState _cypher_state;
    CypherStateData _cypher_data;
};
