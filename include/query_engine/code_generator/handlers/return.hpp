#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto return_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    const auto &elements = action_data.return_elements;
    code += code_line("// number of elements {}", elements.size());

    for (const auto &element : elements)
    {
        auto &entity = element.entity;

        if (!cypher_data.exist(entity)) 
            throw SemanticError(
                fmt::format("{} couldn't be found (RETURN clause).", entity));

        if (element.is_entity_only())
        {
            // if the node has just recently been created on can be found
            // with the internal id then it can be sent to the client
            if (cypher_data.status(entity) == EntityStatus::Created ||
                (cypher_data.source(entity) == EntitySource::InternalId &&
                 cypher_data.status(entity) == EntityStatus::Matched))
            {
                code += code_line(code::write_entity, entity);
            }
            // the client has to receive all elements from the main storage
            if (cypher_data.source(entity) == EntitySource::MainStorage)
            {
                if (cypher_data.type(entity) == EntityType::Node)
                    code += code_line(code::write_all_vertices, entity);
                else if (cypher_data.type(entity) == EntityType::Relationship)
                    code += code_line(code::write_all_edges, entity);

            }
            // the client will receive entities from label index
            if (cypher_data.source(entity) == EntitySource::LabelIndex)
            {
                if (cypher_data.tags(entity).size() == 0)
                    throw CppGeneratorException("node has no labels");

                // node and no other property
                if (cypher_data.type(entity) == EntityType::Node) {
                    // TODO: solve the case with more labels
                    auto label = cypher_data.tags(entity).at(0);  
                    code += code_line(code::find_and_write_vertices_by_label,
                                      entity, label);
                }

                // TODO: 16/10/2016 create match code if properties exist
            }

            if (cypher_data.source(entity) == EntitySource::TypeIndex)
            {
                if (cypher_data.type(entity) == EntityType::Relationship) {
                    if (cypher_data.tags(entity).size() == 0)
                        throw CppGeneratorException("edge has no tag");
                    auto type = cypher_data.tags(entity).at(0);  
                    code += code_line(code::find_and_write_edges_by_type,
                                      entity, type);
                }
            }
        } 
        else if (element.is_projection()) 
        {
            code += code_line("// TODO: implement projection");
            // auto &property = element.property;
            // code += code_line(code::print_property, entity, property);
        }
    }

    // return functions
    for (auto const &kv : action_data.actions)
    {
        auto name = kv.first;

        if (kv.second == ClauseAction::ReturnCount)
        {
            if (cypher_data.source(name) == EntitySource::MainStorage)
            {
                code += code_line(code::count, name);
            }

            if (cypher_data.source(name) == EntitySource::LabelIndex)
            {
                auto tags = cypher_data.tags(name);
                if (tags.size() == 1) {
                    auto label = tags.at(0);
                    code += code_line(code::count_vertices_for_one_label,
                                      name, label);
                }
                // TODO: do for more, isn't easy because of
                // multiple iterators, but we have iterator infrastructure
                // to do that
            }
        }
        if (kv.second == ClauseAction::ReturnLabels)
        {
            // TODO: similar to above
        }
    }

    return code;
};
