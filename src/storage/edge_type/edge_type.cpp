#include "storage/edge_type/edge_type.hpp"

EdgeType::EdgeType(const std::string &id) : EdgeType(std::string(id)) {}
EdgeType::EdgeType(const char *id) : EdgeType(std::string(id)) {}
EdgeType::EdgeType(std::string &&id)
    : id(id), index_v(std::make_unique<type_index_t>(IndexLocation{
                  EdgeSide, Option<std::string>(), Option<std::string>(id)}))
{
}

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

EdgeType::type_index_t &EdgeType::index() const { return *index_v.get(); }
