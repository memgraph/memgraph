#pragma once

#include <string>
#include <tuple>
#include <utility>

#include "cypher/cypher.h"
#include "cypher/tokenizer/cypher_lexer.hpp"
#include "utils/variadic/variadic.hpp"

#include <iostream>

template<typename ...Ts>
class QueryStripper
{
public:

    QueryStripper(Ts&&... strip_types) :
        lexer(std::make_unique<CypherLexer>()),
        strip_types(std::make_tuple(std::forward<Ts>(strip_types)...))
    {
    }
    QueryStripper(QueryStripper& other) = delete;
    QueryStripper(QueryStripper&& other) : 
        strip_types(std::move(other.strip_types)),
        lexer(std::move(other.lexer))
    {
    }

    decltype(auto) strip(const std::string& query)
    {
        //  TODO return hash and arguments
        auto tokenizer = lexer->tokenize(query);
        std::string stripped = "";
        int counter = 0;
        constexpr auto size = std::tuple_size<decltype(strip_types)>::value;
        while (auto token = tokenizer.lookup())
        {
            if (_or(token.id, strip_types, std::make_index_sequence<size>{})) {
                stripped += "@" + std::to_string(counter++);
            } else {
                stripped += token.value;
            }
        }
        return stripped;
    }

private:
    std::tuple<Ts...> strip_types;
    CypherLexer::uptr lexer;

    template<typename Value, typename Tuple, std::size_t ...index>
    bool _or(Value&& value, Tuple&& tuple, std::index_sequence<index...>)
    {
        return or_vargs(std::forward<Value>(value),
                        std::get<index>(std::forward<Tuple>(tuple))...);
    }
};

template<typename ...Ts>
decltype(auto) make_query_stripper(Ts&&... ts) {
    return QueryStripper<Ts...>(std::forward<Ts>(ts)...);
}
