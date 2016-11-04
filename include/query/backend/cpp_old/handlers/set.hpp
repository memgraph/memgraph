#pragma once

#include "includes.hpp"

auto set_query_action = [](CypherStateData &cypher_data,
                           const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions)
    {
        auto name = kv.first;

        if (kv.second == ClauseAction::UpdateNode &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.source(name) == EntitySource::InternalId &&
            cypher_data.type(name) == EntityType::Node)
        {
            code += update_properties(cypher_data, action_data, name);
        }

        if (kv.second == ClauseAction::UpdateNode &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.source(name) == EntitySource::LabelIndex &&
            cypher_data.type(name) == EntityType::Node)
        {
            auto entity_data = action_data.get_entity_property(name);
            for (auto &property : entity_data.properties)
            {
                auto index = action_data.parameter_index.at(
                    ParameterIndexKey(name, property));
                auto tmp_name = name::unique();
                auto label    = cypher_data.tags(name).at(0);
                // TODO: move this code inside the loop (in generated code)
                code += code_line(code::set_vertex_element,
                                  cypher_data.print_indices(name), label,
                                  property, index);
            }
        }

        if (kv.second == ClauseAction::UpdateRelationship &&
            cypher_data.status(name) == EntityStatus::Matched &&
            cypher_data.type(name) == EntityType::Relationship)
        {
            code += update_properties(cypher_data, action_data, name);
        }
    }

    for (auto const &set_entity_labels : action_data.label_set_elements)
    {
        auto &entity = set_entity_labels.entity;

        if (cypher_data.status(entity) == EntityStatus::Matched &&
            cypher_data.source(entity) == EntitySource::LabelIndex)
        {
            auto label = cypher_data.tags(entity).at(0);
            if (cypher_data.has_properties(entity))
            {
                code += code_line(code::set_labels_start,
                                  cypher_data.print_indices(entity), label);
                auto labels = set_entity_labels.labels;
                for (auto const &set_label : labels)
                {
                    code += code_line(code::set_label, set_label);
                }
                code += code_line(code::set_labels_end);
            }
        }
    }

    return code;
};
