// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstddef>
#include <memory>
#include "storage/v2/delta.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/list.hpp"

template <typename Container>
struct Bond {
  explicit Bond(std::size_t initial_size)
      : res_(std::make_unique<resource>(initial_size)),
        container_(memgraph::utils::Allocator<Container>(res_.get()).template new_object<Container>()){};

  Bond(Bond &&other) noexcept : res_(std::move(other.res_)), container_(other.container_) {
    other.container_ = nullptr;
    [[maybe_unused]] auto _ = other.res_.release();
  }

  Bond(const Bond &other) = delete;

  Bond &operator=(const Bond &other) = delete;

  Bond &operator=(Bond &&other) = delete;

  auto use() -> Container & { return *container_; }

  auto use() const -> const Container & { return *container_; }

  auto res() -> memgraph::utils::MonotonicBufferResource * { return res_.get(); }

  ~Bond() {
    if (res_) {
      memgraph::utils::Allocator<Container>(res_.get()).delete_object(container_);
      container_ = nullptr;
      res_->Release();
      res_ = nullptr;
    }
  }

 private:
  using resource = memgraph::utils::MonotonicBufferResource;
  std::unique_ptr<resource> res_{nullptr};
  Container *container_{nullptr};
};

// void func(){
//     using list = memgraph::utils::pmr::list<memgraph::storage::Delta>;
//     Bond<list> my_list;
// }
