#pragma once

#include "utils/option.hpp"
#include "utils/order.hpp"

enum DbSide : uint8_t
{
    EdgeSide = 0,
    VertexSide = 1,
};

struct IndexType
{
public:
    // Are the records unique
    const bool unique;
    // Ordering of the records.
    const Order order;
};

struct IndexLocation
{
public:
    const DbSide side;
    const Option<std::string> property_name;
    const Option<std::string> label_name;
};

// Fully answers:
// On what index?
// What kind of index?
struct IndexDefinition
{
public:
    const IndexLocation loc;
    const IndexType type;
};
