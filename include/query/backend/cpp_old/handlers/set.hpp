#pragma once

#include "includes.hpp"

auto set_query_action = [](CypherStateData &cypher_data,
                           const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {
        auto name = kv.first;

        if (kv.second == ClauseAction::UpdateNode &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.type(name) == EntityType::Node) {
            code += update_properties(cypher_data, action_data, name);
        }

        if (kv.second == ClauseAction::UpdateRelationship &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.type(name) == EntityType::Relationship) {
            code += update_properties(cypher_data, action_data, name);
        }
    }

    return code;
};
