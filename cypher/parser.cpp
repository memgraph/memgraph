#include <cstdlib>
#include <vector>
#include <vector>

#include "compiler.hpp"

int main()
{
    cypher::Compiler compiler;

    //std::string input("MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item");

    std::string input("create n return n");

    compiler.compile(input);

    /* void* parser = cy */
    /* CypherLexer lexer; */

    /* //std::string input("matcH (user:User { name: 'Dominik', age: 8 + 4})-[has:HAS|IS|CAN { duration: 'PERMANENT'}]->(item:Item)--(shop)"); */

    /* std::string input("MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item"); */

    /* auto tokenizer = lexer.tokenize(input); */

    /* std::vector<Token> v; */

    /* while(true) */
    /* { */
    /*     v.push_back(tokenizer.lookup()); */
    /*     auto token = v.back(); */

    /*     //std::cout << token->id << " -> " << token->value << std::endl; */

    /*     auto ast = new ast::Ast(); */
        
    /*     try */
    /*     { */
    /*         cypher_parser(parser, token.id, &token, ast); */
    /*     } */
    /*     catch(...) */
    /*     { */
    /*         std::cout << "caught exception." << std::endl; */
    /*         exit(0); */
    /*     } */

    /*     if(token.id == 0) */
    /*         break; */
    /* } */
    
    /* cypher_parserFree(parser, free); */

    return 0;
}

