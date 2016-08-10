#include "mvcc/id.hpp"

Id::Id(uint64_t id) : id(id) {}

bool operator<(const Id& a, const Id& b)
{
    return a.id < b.id;
}

bool operator==(const Id& a, const Id& b)
{
    return a.id == b.id;
}

std::ostream& operator<<(std::ostream& stream, const Id& id)
{
    return stream << id.id;
}

Id::operator uint64_t() const
{
    return id;
}
