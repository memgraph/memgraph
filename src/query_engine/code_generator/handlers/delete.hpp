#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto delete_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {
        auto entity = kv.first;
        if (kv.second == ClauseAction::DeleteNode) {
            code += LINE(fmt::format("// DELETE Node({})", entity));
        }
        if (kv.second == ClauseAction::DeleteRelationship) {
            code += LINE(fmt::format("// DELETE Relationship({})", entity));
        }
    }

    return code;
};
