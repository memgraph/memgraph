#include <iostream>

#include "antlr4-runtime.h"
#include "query/frontend/opencypher/generated/CypherLexer.h"
#include "query/frontend/opencypher/generated/CypherParser.h"

using namespace antlrcpptest;
using namespace antlr4;

int main(int, const char **a)
{
    const char *query = a[1];

    ANTLRInputStream input(query);
    CypherLexer lexer(&input);
    CommonTokenStream tokens(&lexer);

    tokens.fill();
    for (auto token : tokens.getTokens())
    {
        std::cout << "TYPE: " << token->getType() <<
                     "; TEXT: " << token->getText() <<
                     "; STRING: " << token->toString() << std::endl;
    }

    CypherParser parser(&tokens);
    tree::ParseTree *tree = parser.cypher();

    std::cout << tree->toStringTree(&parser) << std::endl << std::endl;

    return 0;
}

