#pragma once

#include <cstdint>

enum class ClauseAction : uint32_t
{
    Undefined,
    CreateNode,
    MatchNode,
    UpdateNode,
    DeleteNode,
    CreateRelationship,
    MatchRelationship,
    UpdateRelationship,
    DeleteRelationship,
    ReturnNode,
    ReturnRelationship,
    ReturnPack,
    ReturnProjection,
    ReturnCount,
    ReturnLabels,
    
    UpdateEntityLabels,
    UpdateEntityLabels_Identifier,
    UpdateEntityLabels_Labels
};
