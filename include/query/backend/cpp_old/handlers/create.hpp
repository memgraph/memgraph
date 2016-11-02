#pragma once

#include "includes.hpp"

auto create_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    for (auto const &kv : action_data.actions) {

        if (kv.second == ClauseAction::CreateNode) {
            // create node
            auto &name = kv.first;
            code += code_line(code::create_vertex, name);

            // update properties
            code += update_properties(cypher_data, action_data, name);

            // update labels
            auto entity_data = action_data.get_entity_property(name);
            for (auto &label : entity_data.tags) {
                code += code_line(code::create_label, label);
                code += code_line(code::add_label, name, label);
            }

            // mark node as created
            cypher_data.node_created(name);
        }

        if (kv.second == ClauseAction::CreateRelationship) {
            auto name = kv.first;

            // find start and end node
            auto &relationships_data = action_data.relationship_data;
            if (relationships_data.find(name) == relationships_data.end())
                throw CppCodeGeneratorException("Unable to find data for: " +
                                                name);
            auto &relationship_data = relationships_data.at(name);
            auto left_node = relationship_data.nodes.first;
            auto right_node = relationship_data.nodes.second;

            // TODO: If node isn't already matched or created it has to be
            // created here. It is not possible for now.
            if (cypher_data.status(left_node) != EntityStatus::Matched) {
                throw CypherSemanticError("Create Relationship: node " +
                                          left_node + " can't be found");
            }
            if (cypher_data.status(right_node) != EntityStatus::Matched) {
                throw CypherSemanticError("Create Relationship: node " +
                                          right_node + " can't be found");
            }

            // create relationship
            code += code_line(code::create_edge, name, left_node, right_node);

            // update properties
            code += update_properties(cypher_data, action_data, name);

            // update tag
            auto entity_data = action_data.get_entity_property(name);
            for (auto &tag : entity_data.tags) {
                code += code_line(code::find_type, tag);
                code += code_line(code::set_type, name, tag);
            }

            // mark relationship as created
            cypher_data.relationship_created(name);
        }
    }

    return code;
};
