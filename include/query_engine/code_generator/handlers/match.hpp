#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

namespace
{

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
}

auto match_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        auto name = kv.first;

        // TODO: duplicated code -> BIG PROBLEM
        // find node
        if (kv.second == ClauseAction::MatchNode) {
            if (already_matched(cypher_data, name, EntityType::Node))
                continue;
            cypher_data.node_matched(name);
            auto place = action_data.csm.min(name);
            if (place == entity_search::search_internal_id) {
                auto index = fetch_internal_index(action_data, name);
                code += code_line(code::match_vertex_by_id, name, index);
                cypher_data.source(name, EntitySource::InternalId);
            }
            if (place == entity_search::search_main_storage) {
                cypher_data.source(name, EntitySource::MainStorage);
            }
            if (place == entity_search::search_label_index) {
                if (action_data.entity_data.at(name).tags.size() > 1) {
                    throw SemanticError("Multiple label match (currently NOT supported)");
                }
                cypher_data.source(name, EntitySource::LabelIndex);
                cypher_data.tags(name, action_data.entity_data.at(name).tags);
            }
        }

        // find relationship
        if (kv.second == ClauseAction::MatchRelationship) {
            if (already_matched(cypher_data, name, EntityType::Relationship))
                continue;
            cypher_data.relationship_matched(name);
            auto place = action_data.csm.min(name);
            if (place == entity_search::search_internal_id) {
                auto index = fetch_internal_index(action_data, name);
                code += code_line(code::match_edge_by_id, name, index);
                cypher_data.source(name, EntitySource::InternalId);
            }
            if (place == entity_search::search_main_storage) {
                cypher_data.source(name, EntitySource::MainStorage);
            }
            if (place == entity_search::search_type_index) {
                if (action_data.entity_data.at(name).tags.size() > 1) {
                    throw SemanticError("Multiple type match (currently NOT supported)");
                }
                cypher_data.source(name, EntitySource::TypeIndex);
                cypher_data.tags(name, action_data.entity_data.at(name).tags);
            }
        }
    }

    return code;
};
