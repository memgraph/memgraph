#ifndef MEMGRAPH_CYPHER_TOKEN_HPP
#define MEMGRAPH_CYPHER_TOKEN_HPP

#include <ostream>

#include <cstdint>
#include <string>

struct Token
{
    unsigned long id;
    std::string value;

    friend std::ostream& operator<<(std::ostream& stream, const Token& token)
    {
        return stream << "TOKEN id = " << token.id
                      << ", value = '" << token.value << "'";
    }
};

#endif
