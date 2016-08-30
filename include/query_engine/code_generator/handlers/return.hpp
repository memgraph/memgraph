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
                code += code_line(code::write_all_vertices, entity);
            }
            if (cypher_data.source(entity) == EntitySource::LabelIndex)
            {
                // TODO: fetch somehow label name
                // TODO: code_line
            }
        } 
        else if (element.is_projection()) 
        {
            code += code_line("// TODO: implement projection");
            // auto &property = element.property;
            // code += code_line(code::print_property, entity, property);
        }
    }

    return code;
};
