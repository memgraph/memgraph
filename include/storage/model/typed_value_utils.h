//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 01.02.17.
//

#pragma once

#include <ostream>
#include "storage/model/typed_value_store.hpp"


/**
 * Writes all of the values from the given store in JSON format
 * to the given output stream.
 *
 * @param store The store that should be serialized to JSON.
 * @param ostream The stream to write to.
 */
void TypedValuesToJson(const TypedValueStore& store,
                       std::ostream& ostream=std::cout) {

  bool first = true;

  auto write_key = [&ostream, &first](const TypedValueStore::TKey &key) -> std::ostream& {
    if (first) {
      ostream << '{';
      first = false;
    } else
      ostream << ',';

    return ostream << '"' << key << "\":";
  };

  auto handler = [&ostream, &write_key](const TypedValueStore::TKey& key,
                                        const TypedValue& value) {
    switch (value.type_) {
      case TypedValue::Type::Null:
        break;
      case TypedValue::Type::Bool:
        write_key(key) << (value.Value<bool>() ? "true" : "false");
        break;
      case TypedValue::Type::String:
        write_key(key) << '"' << value.Value<std::string>() << '"';
        break;
      case TypedValue::Type::Int:
        write_key(key) << value.Value<int>();
        break;
      case TypedValue::Type::Float:
        write_key(key) << value.Value<float>();
        break;
    }
  };

  auto finish = [&ostream]() { ostream << '}' << std::endl; };

  store.Accept(handler, finish);
}
