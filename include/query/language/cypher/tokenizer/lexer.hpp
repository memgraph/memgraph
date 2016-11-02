#pragma once

#include <cstdint>
#include <memory>

// unfortunatelly, lexertl uses some stuff deprecated in c++11 so we get some
// warnings during compile time, mainly for the auto_ptr
// auto_ptr<lexertl::detail::basic_re_token<char, char> > is deprecated
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "lexertl/generator.hpp"
#include "lexertl/lookup.hpp"
#pragma GCC diagnostic pop

#include "query/language/cypher/errors.hpp"
#include "query/language/cypher/token.hpp"

class Lexer
{
public:

    // public pointer declarations
    using uptr = std::unique_ptr<Lexer>;
    using sptr = std::shared_ptr<Lexer>;

    // constructors
    // default constructor creates unique pointers to object
    // members
    Lexer() :
        rules(std::make_unique<lexertl::rules>()),
        sm(std::make_unique<lexertl::state_machine>())
    {
    }
    // copy constructor is deleted
    Lexer(Lexer& other) = delete;
    // move constructor has default implementation
    Lexer(Lexer&& other) :
        rules(std::move(other.rules)),
        sm(std::move(other.sm))
    {
    }

    //  TODO take care of concurrnecy and moving the lexer object when
    //  some Tokenizer already uses the it (right now I'm not
    //  sure what is going to happen)
    //  check this ASAP
    class Tokenizer
    {
    public:
        Tokenizer(const Lexer& lexer, const std::string& str)
            : lexer(lexer), results(str.begin(), str.end()) {}

        Token lookup()
        {
            lexertl::lookup(*lexer.sm, results);
            auto token = Token {results.id, results.str()};

            if(results.id == static_cast<decltype(results.id)>(-1))
                throw CypherLexicalError(token);

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
        lexertl::generator::build(*rules, *sm);
    }

    void rule(const std::string& regex, uint64_t id)
    {
        rules->push(regex, id);
    }

protected:

    using uptr_lexertl_rules = std::unique_ptr<lexertl::rules>; 
    using uptr_lexertl_sm = std::unique_ptr<lexertl::state_machine>;

    uptr_lexertl_rules rules;
    uptr_lexertl_sm sm;
};
