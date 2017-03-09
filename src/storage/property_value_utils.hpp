//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 01.02.17.
//

#pragma once

#include <ostream>
#include "storage/property_value_store.hpp"

/**
 * Writes all of the values from the given store in JSON format
 * to the given output stream.
 *
 * @param store The store that should be serialized to JSON.
 * @param ostream The stream to write to.
 */
void PropertyValuesToJson(const PropertyValueStore& store,
                          std::ostream& ostream = std::cout) {
  bool first = true;

  auto write_key =
      [&ostream, &first](const PropertyValueStore::TKey& key) -> std::ostream& {
        if (first) {
          ostream << '{';
          first = false;
        } else
          ostream << ',';

        return ostream << '"' << key << "\":";
      };

  auto handler = [&ostream, &write_key](const PropertyValueStore::TKey& key,
                                        const PropertyValue& value) {
    switch (value.type_) {
      case PropertyValue::Type::Null:
        break;
      case PropertyValue::Type::Bool:
        write_key(key) << (value.Value<bool>() ? "true" : "false");
        break;
      case PropertyValue::Type::String:
        write_key(key) << '"' << value.Value<std::string>() << '"';
        break;
      case PropertyValue::Type::Int:
        write_key(key) << value.Value<int64_t>();
        break;
      case PropertyValue::Type::Float:
        write_key(key) << value.Value<float>();
        break;
    }
  };

  auto finish = [&ostream]() { ostream << '}' << std::endl; };

  store.Accept(handler, finish);
}
