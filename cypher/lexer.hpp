#ifndef MEMGRAPH_CYPHER_LEXER_HPP
#define MEMGRAPH_CYPHER_LEXER_HPP

#include <cstdint>

#include "lexertl/generator.hpp"
#include "lexertl/lookup.hpp"

#include "lexical_error.hpp"
#include "token.hpp"

class Lexer
{
public:

    class Tokenizer
    {
    public:
        Tokenizer(const Lexer& lexer, const std::string& str)
            : lexer(lexer), results(str.begin(), str.end()) {}

        Token* lookup()
        {
            lexertl::lookup(lexer.sm, results);
            auto token = new Token {results.id, results.str()};

            if(results.id == static_cast<decltype(results.id)>(-1))
                throw LexicalError(*token);

            return token;
        }

    private:
        const Lexer& lexer;
        lexertl::smatch results;
    };

    Tokenizer tokenize(const std::string& str)
    {
        return Tokenizer(*this, str);
    }

    void build()
    {
        lexertl::generator::build(rules, sm);
    }

    void rule(const std::string& regex, uint64_t id)
    {
        rules.push(regex, id);
    }

protected:
    lexertl::rules rules;
    lexertl::state_machine sm;
};

#endif
