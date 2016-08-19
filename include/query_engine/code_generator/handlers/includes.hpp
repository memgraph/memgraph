#pragma once

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "query_engine/code_generator/cypher_state.hpp"
#include "query_engine/code_generator/namer.hpp"
#include "query_engine/code_generator/query_action_data.hpp"
#include "query_engine/exceptions/errors.hpp"
#include "query_engine/traverser/code.hpp"
#include "query_engine/util.hpp"

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
        code += code_line((cypher_state.type(name) == EntityType::Node
                               ? code::vertex_property_key
                               : code::edge_property_key),
                          tmp_name, property, index);
        code += code_line(code::set_property, name, tmp_name, index);
    }

    return code;
}
}
