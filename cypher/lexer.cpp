#include "lexertl/generator.hpp"
#include <iostream>
#include "lexertl/lookup.hpp"

#include "cypher_lexer.hpp"

int main()
{
    CypherLexer lexer;

    auto tokenizer = lexer.tokenize("{name: 'Dominik', lastName: 'Tomicevic', age: 24 }");

    while(true)
    {
        auto token = tokenizer.lookup();

        if(token.id == 0)
            break;

        std::cout << token.id << " -> " << token.value << std::endl;
    }


    lexertl::rules rules;
    lexertl::state_machine sm;

    rules.push("\\s+", sm.skip());
    rules.push("MATCH", 1);
    rules.push("RETURN", 2);

    rules.push("'(.*?)'", 4); // string literal TODO single quote escape
    rules.push("\\\"(.*?)\\\"", 4); // string literal TODO double quote escape
    rules.push("[-+]?(\\d*[.])?\\d+", 5); // number

    rules.push("[_a-zA-Z][_a-zA-Z0-9]{0,30}", 3); // identifier

    lexertl::generator::build(rules, sm);
   
    std::string input("MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13', AND item.price = 14.99 RETURN user, has, item");
    lexertl::smatch results(input.begin(), input.end());

    // Read ahead
    lexertl::lookup(sm, results);

    while (results.id != 0)
    {
        std::cout << "Id: " << results.id << ", Token: '" <<
            results.str () << "'\n";
        lexertl::lookup(sm, results);
    }

    std::cout << "-1 to uint64_t = " << uint64_t(-1) << std::endl;

    return 0;
}

