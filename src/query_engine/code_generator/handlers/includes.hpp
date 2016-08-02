#pragma once

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "query_engine/util.hpp"
#include "query_engine/code_generator/cypher_state.hpp"
#include "query_engine/code_generator/query_action_data.hpp"
#include "query_engine/traverser/code.hpp"
#include "query_engine/exceptions/errors.hpp"

using ParameterIndexKey::Type::InternalId;
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
