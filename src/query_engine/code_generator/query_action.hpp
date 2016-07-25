#pragma once

#include <cstdint>

// any entity (node or relationship) inside cypher query has an action
// that is associated with that entity
enum class QueryAction : uint32_t
{
    TransactionBegin,
    Create,
    Match,
    Set,
    Return,
    Delete,
    TransactionCommit
};
