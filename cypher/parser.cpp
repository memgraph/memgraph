#include <cstdlib>
#include <vector>
#include <vector>

#include "cypher.h"
#include "token.hpp"

#include "cypher_lexer.hpp"
#include "ast/tree.hpp"

void* cypher_parserAlloc(void* (*allocProc)(size_t));
void  cypher_parser(void*, int, Token*, ast::Ast* ast);
void  cypher_parserFree(void*, void(*freeProc)(void*));

int main()
{
    void* parser = cypher_parserAlloc(malloc);
    CypherLexer lexer;

    //std::string input("matcH (user:User { name: 'Dominik', age: 8 + 4})-[has:HAS|IS|CAN { duration: 'PERMANENT'}]->(item:Item)--(shop)");

    std::string input("MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item");

    auto tokenizer = lexer.tokenize(input);

    std::vector<Token*> v;

    while(true)
    {
        v.push_back(tokenizer.lookup());
        auto token = v.back();

        //std::cout << token->id << " -> " << token->value << std::endl;

        auto ast = new ast::Ast();
        
        try
        {
            cypher_parser(parser, token->id, token, ast);
        }
        catch(...)
        {
            std::cout << "caught exception." << std::endl;
            exit(0);
        }

        if(token->id == 0)
            break;
    }
    
    cypher_parserFree(parser, free);

    return 0;
}

