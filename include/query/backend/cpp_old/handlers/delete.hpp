#pragma once

#include "includes.hpp"

auto delete_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {
        auto entity = kv.first;
        if (kv.second == ClauseAction::DeleteNode && action_data.is_detach) {
            code += code_line(delete_whole_graph);
        }
        if (kv.second == ClauseAction::DeleteNode && !action_data.is_detach) {
            code += code_line(delete_all_detached_nodes);
        }
        if (kv.second == ClauseAction::DeleteRelationship) {
            code += code_line("// DELETE Relationship({})", entity);
        }
    }

    return code;
};
