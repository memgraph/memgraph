#ifndef MEMGRAPH_CYPHER_TOKEN_HPP
#define MEMGRAPH_CYPHER_TOKEN_HPP

#include <cstdint>
#include <string>

struct Token
{
    unsigned long id;
    std::string value;
};

#endif
