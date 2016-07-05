#pragma once

#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "cypher/cypher.h"
#include "cypher/tokenizer/cypher_lexer.hpp"
#include "utils/hashing/fnv.hpp"
#include "query_stripped.hpp"
#include "storage/model/properties/all.hpp"
#include "utils/string/transform.hpp"
#include "utils/variadic/variadic.hpp"


template <class T, class V>
void store_query_param(code_args_t &arguments, V &&v)
{
    arguments.emplace_back(std::make_shared<T>(std::forward<V>(v)));
}

template <typename... Ts>
class QueryStripper
{
public:
    QueryStripper(Ts &&... strip_types)
        : strip_types(std::make_tuple(std::forward<Ts>(strip_types)...)),
          lexer(std::make_unique<CypherLexer>())
    {
    }

    QueryStripper(QueryStripper &other) = delete;

    QueryStripper(QueryStripper &&other)
        : strip_types(std::move(other.strip_types)),
          lexer(std::move(other.lexer))
    {
    }

    auto strip(const std::string &query)
    {
        //  TODO write this more optimal (resplace string
        //  concatenation with something smarter)
        //  TODO: in place substring replacement

        auto tokenizer = lexer->tokenize(query);

        // TMP size of supported token types
        constexpr auto size = std::tuple_size<decltype(strip_types)>::value;

        int counter = 0;
        code_args_t stripped_arguments;
        std::string stripped_query;
        stripped_query.reserve(query.size());

        while (auto token = tokenizer.lookup()) {
            // TODO: better implementation
            if (_or(token.id, strip_types, std::make_index_sequence<size>{})) {
                auto index = counter++;
                switch (token.id) {
                case TK_INT:
                    store_query_param<Int32>(stripped_arguments,
                                             std::stoi(token.value));
                    break;
                case TK_STR:
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
                }
                stripped_query += std::to_string(index);
            } else {
                //  TODO: lowercase only keywords like (MATCH, CREATE, ...)
                stripped_query += token.value;
            }
        }

        return QueryStripped(std::move(stripped_query),
                             fnv(stripped_query),
                             std::move(stripped_arguments));
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
