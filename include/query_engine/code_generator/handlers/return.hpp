#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto return_query_action =
    [](CypherStateData &cypher_data,
       const QueryActionData &action_data) -> std::string {

    std::string code = "";

    const auto &elements = action_data.return_elements;
    code += code_line("// number of elements {}", elements.size());

    // TODO: call bolt serialization
    for (const auto &element : elements) {
        auto &entity = element.entity;
        if (!cypher_data.exist(entity)) {
            throw SemanticError(
                fmt::format("{} couldn't be found (RETURN clause).", entity));
        }
        if (element.is_entity_only()) {
            code += code_line(code::write_entity, entity);
        } else if (element.is_projection()) {
            code += code_line("// TODO: implement projection");
            // auto &property = element.property;
            // code += code_line(code::print_property, entity, property);
        }
    }

    return code;
};
