// #include "storage/indexes/impl/nonunique_unordered_index.hpp"
#include "storage/label/label.hpp"

Label::Label(const char *name)
    : name(std::string(name)),
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

bool operator<(const CharStr &lhs, const Label &rhs)
{
    return lhs < rhs.char_str();
}

bool operator==(const CharStr &lhs, const Label &rhs)
{
    return lhs == rhs.char_str();
}

std::ostream &operator<<(std::ostream &stream, const Label &label)
{
    return stream << label.name;
}

Label::operator const std::string &() const { return name; }
