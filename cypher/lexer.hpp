#ifndef MEMGRAPH_CYPHER_LEXER_HPP
#define MEMGRAPH_CYPHER_LEXER_HPP

#include <cstdint>

// unfortunatelly, lexertl uses some stuff deprecated in c++11 so we get some
// warnings during compile time, mainly for the auto_ptr
// auto_ptr<lexertl::detail::basic_re_token<char, char> > is deprecated
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "lexertl/lexertl/generator.hpp"
#include "lexertl/lexertl/lookup.hpp"
#pragma GCC diagnostic pop

#include "errors.hpp"
#include "token.hpp"

class Lexer
{
public:
    class Tokenizer
    {
    public:
        Tokenizer(const Lexer& lexer, const std::string& str)
            : lexer(lexer), results(str.begin(), str.end()) {}

        Token lookup()
        {
            lexertl::lookup(lexer.sm, results);
            auto token = Token {results.id, results.str()};

            if(results.id == static_cast<decltype(results.id)>(-1))
                throw LexicalError(token);

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
