#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto set_query_action = [](CypherStateData &cypher_data,
                           const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {
        auto name = kv.first;
        if (kv.second == ClauseAction::UpdateNode &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.type(name) == EntityType::Node) {
            auto entity_data = action_data.get_entity_property(name);
            for (auto &property : entity_data.properties) {
                auto index = action_data.parameter_index.at(
                    ParameterIndexKey(name, property));
                code += LINE(
                    fmt::format(code::update_property, name, property, index));
            }
        }
    }

    return code;
};
