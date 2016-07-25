#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto create_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        if (kv.second == ClauseAction::CreateNode) {
            // 1. create node 2. update labels 3. update properties
            auto &name = kv.first;
            code += LINE(fmt::format(code::create_vertex, name));
            auto entity_data = action_data.get_entity_property(name);
            for (auto &property : entity_data.properties) {
                auto index = action_data.parameter_index.at(
                    ParameterIndexKey(name, property));
                code += LINE(fmt::format(code::set_vertex_property, name,
                                         property, index));
            }
            for (auto &label : entity_data.tags) {
                code += LINE(fmt::format(code::create_label, label));
                code += LINE(fmt::format(code::add_label, name, label));
            }
            cypher_data.node_created(name);
        }

        if (kv.second == ClauseAction::CreateRelationship) {
            auto name = kv.first;
            code += LINE(fmt::format(code::create_edge, name));
            cypher_data.relationship_created(name);
        }
    }

    return code;
};
