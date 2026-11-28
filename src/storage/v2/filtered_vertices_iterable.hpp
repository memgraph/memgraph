// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#ifdef MG_ENTERPRISE

#include <memory>
#include <optional>
#include <type_traits>
#include <variant>

#include "storage/v2/vertex_row_filter.hpp"
#include "storage/v2/vertices_iterable.hpp"

namespace memgraph::storage {

/// A label or label+property index scan whose skip loop also drops the rows a VertexRowFilter hides: the same iterators
/// as `VerticesIterable`'s, instantiated with their `Filtered` flag. It is a type of its own, so `VerticesIterable` and
/// its iterators, which main uses, contain nothing of it.
class FilteredVerticesIterable final {
  using LabelIterable = InMemoryLabelIndex::FilteredIterable;
  template <typename EntryT>
  using PropertyIterable = InMemoryLabelPropertyIndex::Iterable<EntryT, true>;
  using Data = std::variant<LabelIterable, PropertyIterable<InMemoryLabelPropertyIndex::Entry<>>,
                            PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<>>,
                            PropertyIterable<InMemoryLabelPropertyIndex::Entry<1>>,
                            PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<1>>,
                            PropertyIterable<InMemoryLabelPropertyIndex::Entry<2>>,
                            PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<2>>>;

  Data data_;

  explicit FilteredVerticesIterable(Data data) : data_(std::move(data)) {}

 public:
  /// `base` with `filter` set, or empty (and `base` untouched) for any scan but the label and label+property index
  /// ones.
  static std::optional<FilteredVerticesIterable> Make(VerticesIterable &&base,
                                                      std::shared_ptr<VertexRowFilter const> filter) {
    return std::visit(
        [&](auto &v) -> std::optional<FilteredVerticesIterable> {
          using T = std::decay_t<decltype(v)>;
          auto const made = [&]<typename Filtered>() {
            return FilteredVerticesIterable(Data(std::in_place_type<Filtered>, std::move(v), std::move(filter)));
          };
          if constexpr (std::is_same_v<T, InMemoryLabelIndex::Iterable>) {
            return made.template operator()<LabelIterable>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::AscIterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::Entry<>>>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::DescIterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<>>>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::Asc1Iterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::Entry<1>>>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::Desc1Iterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<1>>>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::Asc2Iterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::Entry<2>>>();
          } else if constexpr (std::is_same_v<T, VerticesIterable::Desc2Iterable>) {
            return made.template operator()<PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<2>>>();
          } else {
            return std::nullopt;
          }
        },
        base.data_);
  }

  class Iterator final {
    using IteratorData =
        std::variant<LabelIterable::Iterator, PropertyIterable<InMemoryLabelPropertyIndex::Entry<>>::Iterator,
                     PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<>>::Iterator,
                     PropertyIterable<InMemoryLabelPropertyIndex::Entry<1>>::Iterator,
                     PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<1>>::Iterator,
                     PropertyIterable<InMemoryLabelPropertyIndex::Entry<2>>::Iterator,
                     PropertyIterable<InMemoryLabelPropertyIndex::DescEntry<2>>::Iterator>;

    IteratorData data_;

   public:
    using difference_type = std::ptrdiff_t;
    using value_type = VertexAccessor;

    template <typename It>
    explicit Iterator(It it) : data_(std::move(it)) {}

    value_type const &operator*() const {
      return std::visit([](auto const &it) -> value_type const & { return *it; }, data_);
    }

    Iterator &operator++() {
      std::visit([](auto &it) { ++it; }, data_);
      return *this;
    }

    bool operator==(const Iterator &other) const = default;
  };

  Iterator begin() {
    return std::visit([](auto &v) -> Iterator { return Iterator(v.begin()); }, data_);
  }

  Iterator end() {
    return std::visit([](auto &v) -> Iterator { return Iterator(v.end()); }, data_);
  }
};

}  // namespace memgraph::storage

#endif
