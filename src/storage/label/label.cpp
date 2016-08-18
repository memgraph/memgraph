// #include "storage/indexes/impl/nonunique_unordered_index.hpp"
#include "storage/label/label.hpp"

Label::Label(const std::string &name)
    : name(name), index(std::unique_ptr<label_index_t>(new label_index_t()))
{
}
Label::Label(std::string &&name)
    : name(std::move(name)),
      index(std::unique_ptr<label_index_t>(new label_index_t()))
{
}

bool operator<(const Label &lhs, const Label &rhs)
{
    return lhs.name < rhs.name;
}

bool operator==(const Label &lhs, const Label &rhs)
{
    return lhs.name == rhs.name;
}

std::ostream &operator<<(std::ostream &stream, const Label &label)
{
    return stream << label.name;
}

Label::operator const std::string &() const { return name; }
