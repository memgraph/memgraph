#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

bool already_matched(CypherStateData &cypher_data, const std::string &name,
                     EntityType type)
{
    if (cypher_data.type(name) == type &&
        cypher_data.status(name) == EntityStatus::Matched)
        return true;
    else
        return false;
}

auto fetch_internal_index(const QueryActionData &action_data,
                          const std::string &name)
{
    return action_data.parameter_index.at(ParameterIndexKey(InternalId, name));
}

auto match_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        // TODO: the same code REFACTOR!
        // find node
        if (kv.second == ClauseAction::MatchNode) {
            auto name = kv.first;
            if (already_matched(cypher_data, name, EntityType::Node)) continue;
            cypher_data.node_matched(name);
            auto place = action_data.csm.min(kv.first);
            if (place == entity_search::search_internal_id) {
                auto index = fetch_internal_index(action_data, name);
                code +=
                    LINE(fmt::format(code::match_vertex_by_id, name, index));
            }
        }

        // find relationship
        if (kv.second == ClauseAction::MatchRelationship) {
            auto name = kv.first;
            if (already_matched(cypher_data, name, EntityType::Relationship))
                continue;
            cypher_data.relationship_matched(name);
            auto place = action_data.csm.min(kv.first);
            if (place == entity_search::search_internal_id) {
                auto index = fetch_internal_index(action_data, name);
                code += LINE(fmt::format(code::match_edge_by_id, name, index));
            }
        }
    }

    return code;
};
