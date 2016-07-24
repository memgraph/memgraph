#pragma once

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "query_engine/code_generator/cypher_state.hpp"
#include "query_engine/code_generator/query_action_data.hpp"
#include "query_engine/traverser/code.hpp"

using ParameterIndexKey::Type::InternalId;

// any entity (node or relationship) inside cypher query has an action
// that is associated with that entity
enum class QueryAction : uint32_t
{
    TransactionBegin,
    Create,
    Match,
    Set,
    Return,
    TransactionCommit
};

auto transaction_begin_action = [](CypherStateData &,
                                   const QueryActionData &) -> std::string {
    return LINE(code::transaction_begin);
};

auto create_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        if (kv.second == ClauseAction::CreateRelationship) {
            auto name = kv.first;
            code += LINE(fmt::format(code::create_edge, name));
            cypher_data.relationship_created(name);
        }
    }

    return code;
};

auto match_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        if (kv.second == ClauseAction::MatchNode) {
            auto name = kv.first;
            if (cypher_data.type(name) == EntityType::Node &&
                cypher_data.status(name) == EntityStatus::Matched)
                continue;
        }

        // find node
        if (kv.second == ClauseAction::MatchNode) {
            auto name = kv.first;
            auto place = action_data.csm.min(kv.first);
            if (place == entity_search::search_internal_id) {
                auto index = action_data.parameter_index.at(
                    ParameterIndexKey(InternalId, name));
                code +=
                    LINE(fmt::format(code::match_vertex_by_id, name, index));
                cypher_data.node_matched(name);
            }
        }
    }

    return code;
};

auto set_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {
        auto name = kv.first;
        if (kv.second == ClauseAction::UpdateNode &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.type(name) == EntityType::Node) {
            auto entity_data = action_data.get_entity_property(name);
            for (auto &property : entity_data.properties) {
                auto index = action_data.parameter_index.at(ParameterIndexKey(name, property));
               code += LINE(fmt::format(code::update_property, name, property, index)); 
            }
        }
    }

    return code;
};

auto return_query_action = [](
    CypherStateData &, const QueryActionData &) -> std::string { return ""; };

auto transaction_commit_action = [](CypherStateData &,
                                    const QueryActionData &) -> std::string {
    return LINE(code::transaction_commit) + LINE(code::return_empty_result);
};
