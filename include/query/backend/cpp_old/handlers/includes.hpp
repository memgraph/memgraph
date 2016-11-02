#pragma once

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "query/backend/cpp_old/cypher_state.hpp"
#include "query/backend/cpp_old/namer.hpp"
#include "query/backend/cpp_old/query_action_data.hpp"
#include "query/backend/cpp_old/code.hpp"
#include "query/util.hpp"
#include "query/exception/cpp_code_generator.hpp"
#include "query/language/cypher/errors.hpp"

using ParameterIndexKey::Type::InternalId;
using Direction = RelationshipData::Direction;

namespace
{

auto update_properties(const CypherStateData &cypher_state,
                       const QueryActionData &action_data,
                       const std::string &name)
{
    std::string code = "";

    auto entity_data = action_data.get_entity_property(name);
    for (auto &property : entity_data.properties) {
        auto index =
            action_data.parameter_index.at(ParameterIndexKey(name, property));
        auto tmp_name = name::unique();
        // TODO: ERROR! why
        code += code_line((cypher_state.type(name) == EntityType::Node
                               ? code::vertex_property_key
                               : code::edge_property_key),
                          tmp_name, property, index);
        code += code_line(code::set_property, name, tmp_name, index);
    }

    return code;
}
}
