#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

using Direction = RelationshipData::Direction;

auto update_properties(const QueryActionData &action_data,
                       const std::string &name)
{
    std::string code = "";

    auto entity_data = action_data.get_entity_property(name);
    for (auto &property : entity_data.properties) {
        auto index =
            action_data.parameter_index.at(ParameterIndexKey(name, property));
        code += LINE(fmt::format(code::set_property, name, property, index));
    }

    return code;
}

auto create_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        if (kv.second == ClauseAction::CreateNode) {
            // create node
            auto &name = kv.first;
            code += LINE(fmt::format(code::create_vertex, name));

            // update properties
            code += update_properties(action_data, name);

            // update labels
            auto entity_data = action_data.get_entity_property(name);
            for (auto &label : entity_data.tags) {
                code += LINE(fmt::format(code::create_label, label));
                code += LINE(fmt::format(code::add_label, name, label));
            }

            // mark node as created
            cypher_data.node_created(name);
        }

        if (kv.second == ClauseAction::CreateRelationship) {
            // create relationship
            auto name = kv.first;
            code += LINE(fmt::format(code::create_edge, name));

            // update properties
            code += update_properties(action_data, name);

            // update tag
            auto entity_data = action_data.get_entity_property(name);
            for (auto &tag : entity_data.tags) {
                code += LINE(fmt::format(code::find_type, tag));
                code += LINE(fmt::format(code::set_type, name, tag));
            }

            // find start and end node
            auto &relationships_data = action_data.relationship_data;
            if (relationships_data.find(name) == relationships_data.end())
                throw CodeGenerationError("Unable to find data for: " + name);
            auto &relationship_data = relationships_data.at(name);
            auto left_node = relationship_data.nodes.first;
            auto right_node = relationship_data.nodes.second;

            // TODO: If node isn't already matched or created it has to be
            // created here. It is not possible for now.
            if (cypher_data.status(left_node) != EntityStatus::Matched) {
                throw SemanticError("Create Relationship: node " + left_node +
                                    " can't be found");
            }
            if (cypher_data.status(right_node) != EntityStatus::Matched) {
                throw SemanticError("Create Relationship: node " + right_node +
                                    " can't be found");
            }

            // define direction
            if (relationship_data.direction == Direction::Right) {
                code += LINE(fmt::format(code::node_out, left_node, name));
                code += LINE(fmt::format(code::node_in, right_node, name));
                code += LINE(fmt::format(code::edge_from, name, left_node));
                code += LINE(fmt::format(code::edge_to, name, right_node));
            } else if (relationship_data.direction == Direction::Left) {
                code += LINE(fmt::format(code::node_out, right_node, name));
                code += LINE(fmt::format(code::node_in, left_node, name));
                code += LINE(fmt::format(code::edge_from, name, right_node));
                code += LINE(fmt::format(code::edge_to, name, left_node));
            }

            // mark relationship as created
            cypher_data.relationship_created(name);
        }
    }

    return code;
};
