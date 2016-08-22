#include "storage/edge_type/edge_type.hpp"

EdgeType::EdgeType() {}
EdgeType::EdgeType(const std::string &id) : id(id) {}
EdgeType::EdgeType(const char *id) : id(std::string(id)) {}
EdgeType::EdgeType(std::string &&id) : id(std::move(id)) {}

bool operator<(const EdgeType &lhs, const EdgeType &rhs)
{
    return lhs.id < rhs.id;
}

bool operator==(const EdgeType &lhs, const EdgeType &rhs)
{
    return lhs.id == rhs.id;
}

std::ostream &operator<<(std::ostream &stream, const EdgeType &type)
{
    return stream << type.id;
}

EdgeType::operator const std::string &() const { return id; }
