#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "cypher/cypher.h"
#include "logging/loggable.hpp"
#include "query/language/cypher/tokenizer/cypher_lexer.hpp"
#include "query/strip/stripped.hpp"
#include "storage/model/properties/all.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string/transform.hpp"
#include "utils/variadic/variadic.hpp"

// TODO: Maybe std::move(v) is faster, but it must be cheked for validity.
template <class T, class V>
void store_query_param(plan_args_t &arguments, V &&v)
{
    arguments.emplace_back(Property(T(std::move(v)), T::type));
}

template <typename... Ts>
class QueryStripper : public Loggable
{
public:
    QueryStripper(Ts &&... strip_types) : Loggable("QueryStripper"),
          strip_types(std::make_tuple(std::forward<Ts>(strip_types)...)),
          lexer(std::make_unique<CypherLexer>())
    {
    }

    QueryStripper(QueryStripper &other) = delete;

    QueryStripper(QueryStripper &&other) : Loggable("QueryStripper"),
          strip_types(std::move(other.strip_types)),
          lexer(std::move(other.lexer))
    {
    }

    auto strip_space(const std::string &query)
    {
        return strip(query, " ");
    }

    auto strip(const std::string &query, const std::string &separator = "")
    {
        //  TODO write this more optimal (resplace string
        //  concatenation with something smarter)
        //  TODO: in place substring replacement

        auto tokenizer = lexer->tokenize(query);

        // TMP size of supported token types
        constexpr auto size = std::tuple_size<decltype(strip_types)>::value;

        int counter = 0;
        plan_args_t stripped_arguments;
        std::string stripped_query;
        stripped_query.reserve(query.size());

        while (auto token = tokenizer.lookup())
        {
            if (_or(token.id, strip_types, std::make_index_sequence<size>{}))
            {
                auto index = counter++;
                switch (token.id) {
                case TK_LONG:
                    store_query_param<Int64>(stripped_arguments,
                                             std::stol(token.value));
                    break;
                case TK_STR:
                    // TODO: remove quotes view lexertl
                    token.value.erase(0, 1);
                    token.value.erase(token.value.length() - 1, 1);
                    // TODO: remove
                    store_query_param<String>(stripped_arguments, token.value);
                    break;
                case TK_BOOL: {
                    bool value = token.value[0] == 'T' || token.value[0] == 't';
                    store_query_param<Bool>(stripped_arguments, value);
                    break;
                }
                case TK_FLOAT:
                    store_query_param<Float>(stripped_arguments,
                                             std::stof(token.value));
                    break;
                default:
                    // TODO: other properties
                    assert(false);
                }
                stripped_query += std::to_string(index) + separator;
            } else {
                //  TODO: lowercase only keywords like (MATCH, CREATE, ...)
                stripped_query += token.value + separator;
            }
        }

        auto hash = fnv(stripped_query);
        return QueryStripped(std::move(stripped_query),
                             std::move(stripped_arguments),
                             hash);
    }

private:
    std::tuple<Ts...> strip_types;
    CypherLexer::uptr lexer;

    template <typename Value, typename Tuple, std::size_t... index>
    bool _or(Value &&value, Tuple &&tuple, std::index_sequence<index...>)
    {
        return or_vargs(std::forward<Value>(value),
                        std::get<index>(std::forward<Tuple>(tuple))...);
    }
};

template <typename... Ts>
decltype(auto) make_query_stripper(Ts &&... ts)
{
    return QueryStripper<Ts...>(std::forward<Ts>(ts)...);
}
