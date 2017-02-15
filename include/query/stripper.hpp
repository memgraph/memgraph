#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "cypher/cypher.h"
#include "logging/loggable.hpp"
#include "query/language/cypher/tokenizer/cypher_lexer.hpp"
#include "query/stripped.hpp"
#include "storage/typed_value_store.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string/transform.hpp"
#include "utils/variadic/variadic.hpp"

// TODO: all todos will be resolved once Antler will be integrated
template<typename... Ts>
class QueryStripper : public Loggable {
public:
  QueryStripper(Ts &&... strip_types)
      : Loggable("QueryStripper"),
        strip_types(std::make_tuple(std::forward<Ts>(strip_types)...)),
        lexer(std::make_unique<CypherLexer>()) {
  }

  QueryStripper(QueryStripper &other) = delete;

  QueryStripper(QueryStripper &&other)
      : Loggable("QueryStripper"), strip_types(std::move(other.strip_types)),
        lexer(std::move(other.lexer)) {
  }

  auto strip(const std::string &query, const std::string &separator = " ") {
    //  -------------------------------------------------------------------
    //  TODO: write speed tests and then optimize, because this
    //  function is called before every query execution !
    //  -------------------------------------------------------------------

    //  TODO write this more optimal (resplace string
    //  concatenation with something smarter)
    //  TODO: in place substring replacement

    auto tokenizer = lexer->tokenize(query);

    // TMP size of supported token types
    constexpr auto size = std::tuple_size<decltype(strip_types)>::value;

    TypedValueStore<> stripped_arguments;
    std::string stripped_query;
    stripped_query.reserve(query.size());

    int counter = 0; // how many arguments have we processed so far
    while (auto token = tokenizer.lookup()) {
      if (_or(token.id, strip_types, std::make_index_sequence < size > {})) {
        switch (token.id) {
          case TK_LONG:
            stripped_arguments.set(counter, std::stoi(token.value));
            break;
          case TK_STR:
            // TODO: remove quotes view lexertl
            token.value.erase(0, 1);
            token.value.erase(token.value.length() - 1, 1);
            // TODO: remove
            stripped_arguments.set(counter, token.value);
            break;
          case TK_BOOL: {
            bool value = token.value[0] == 'T' || token.value[0] == 't';
            stripped_arguments.set(counter, value);
            break;
          }
          case TK_FLOAT:
            stripped_arguments.set(counter, std::stof(token.value));
            break;
          default:
            // TODO: other properties
            assert(false);
        }
        stripped_query += std::to_string(counter++) + separator;
      } else {
        // if token is keyword then lowercase because query hash
        // should be the same
        // TODO: probably we shoud do the lowercase before
        // or during the tokenization (SPEED TESTS)
        if (token.id == TK_OR || token.id == TK_AND ||
            token.id == TK_NOT || token.id == TK_WITH ||
            token.id == TK_SET || token.id == TK_CREATE ||
            token.id == TK_MERGE || token.id == TK_MATCH ||
            token.id == TK_DELETE || token.id == TK_DETACH ||
            token.id == TK_WHERE || token.id == TK_RETURN ||
            token.id == TK_DISTINCT || token.id == TK_COUNT ||
            token.id == TK_LABELS) {
          std::transform(token.value.begin(), token.value.end(),
                         token.value.begin(), ::tolower);
        }
        stripped_query += token.value + separator;
      }
    }

    // TODO: hash function should be a template parameter
    HashType hash = fnv(stripped_query);
    return StrippedQuery(std::move(stripped_query),
                         std::move(stripped_arguments), hash);
  }

private:
  std::tuple<Ts...> strip_types;
  CypherLexer::uptr lexer;

  template<typename Value, typename Tuple, std::size_t... index>
  bool _or(Value &&value, Tuple &&tuple, std::index_sequence<index...>) {
    return utils::or_vargs(std::forward<Value>(value),
                           std::get<index>(std::forward<Tuple>(tuple))...);
  }
};

template<typename... Ts>
decltype(auto) make_query_stripper(Ts &&... ts) {
  return QueryStripper<Ts...>(std::forward<Ts>(ts)...);
}
