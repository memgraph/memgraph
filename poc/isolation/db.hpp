#include <string>

namespace base
{
class Accessor
{
public:
    char before = ~((char)-1);
    int data = 0;
    size_t after = ~((size_t)-1);
};

class Name
{
public:
    Name(const char *str) : name(std::string(str)) {}

    std::string name;
};

class Db
{
public:
    int accessed = 0;
    int data = 0;
    Name name = {"name"};
};
}
