// Copyright 2024 Memgraph Ltd.
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

#include <memory>
#include <vector>

#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace memgraph::query {

/**
 * `AnyStream` can wrap *any* type implementing the `Stream` concept into a
 * single type.
 *
 * The type erasure technique is used. The original type which an `AnyStream`
 * was constructed from is "erased", as `AnyStream` is not a class template and
 * doesn't use the type in any way. Client code can then program just for
 * `AnyStream`, rather than using static polymorphism to handle any type
 * implementing the `Stream` concept.
 */
class AnyStream final {
 public:
  template <class TStream>
  AnyStream(TStream *stream, utils::MemoryResource *memory_resource)
      : content_{
            utils::Allocator<GenericWrapper<TStream>>{memory_resource}.template new_object<GenericWrapper<TStream>>(
                stream),
            [memory_resource](Wrapper *ptr) {
              utils::Allocator<GenericWrapper<TStream>>{memory_resource}
                  .template delete_object<GenericWrapper<TStream>>(static_cast<GenericWrapper<TStream> *>(ptr));
            }} {}

  void Result(const std::vector<TypedValue> &values) { content_->Result(values); }

 private:
  struct Wrapper {
    virtual void Result(const std::vector<TypedValue> &values) = 0;
    virtual ~Wrapper() = default;
  };

  template <class TStream>
  struct GenericWrapper final : public Wrapper {
    explicit GenericWrapper(TStream *stream) : stream_{stream} {}

    void Result(const std::vector<TypedValue> &values) override { stream_->Result(values); }

    TStream *stream_;
  };

  std::unique_ptr<Wrapper, std::function<void(Wrapper *)>> content_;
};

}  // namespace memgraph::query
