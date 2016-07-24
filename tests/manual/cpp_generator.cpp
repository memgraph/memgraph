#include <iostream>

#include "query_engine/code_generator/cpp_generator.hpp"

using ParameterIndexKey::Type::InternalId;
using ParameterIndexKey::Type::Projection;

auto main() -> int
{
    CppGenerator generator;
    
    // MATCH
    generator.add_action(QueryAction::Match);

    auto& data_match = generator.action_data();
    data_match.actions["n1"] = ClauseAction::MatchNode;
    data_match.actions["n2"] = ClauseAction::MatchNode;

    // CREATE
    generator.add_action(QueryAction::Create);

    auto& data_create = generator.action_data();
    data_create.actions["r"] = ClauseAction::CreateRelationship;

    // RETURN
    generator.add_action(QueryAction::Return);

    auto& data_return = generator.action_data();
    data_return.actions["r"] = ClauseAction::ReturnRelationship;

    generator.generate();

    return 0;
}
