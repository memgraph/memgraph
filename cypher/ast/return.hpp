#ifndef MEMGRAPH_CYPHER_AST_RETURN_HPP
#define MEMGRAPH_CYPHER_AST_RETURN_HPP

#include "list.hpp"
#include "identifier.hpp"

namespace ast
{

struct ReturnList : public List<Identifier, ReturnList>
{
    using List::List;
};

};

#endif
