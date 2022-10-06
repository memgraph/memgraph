// Copyright 2022 Memgraph Ltd.
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

#include <map>
#include <optional>
#include <variant>

#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "storage/v2/storage.hpp"
#include "utils/logging.hpp"

namespace memgraph::tests::simulation {

struct CreateVertex {
  int key;
  int value;

  friend std::ostream &operator<<(std::ostream &in, const CreateVertex &add) {
    in << "CreateVertex { key: " << add.key << ", value: " << add.value << " }";
    return in;
  }
};

struct ScanAll {
  friend std::ostream &operator<<(std::ostream &in, const ScanAll &get) {
    in << "ScanAll {}";
    return in;
  }
};

using OpVariant = std::variant<CreateVertex, ScanAll>;

struct Op {
  OpVariant inner;

  friend std::ostream &operator<<(std::ostream &in, const Op &op) {
    std::visit([&](const auto &x) { in << x; }, op.inner);
    return in;
  }
};

}  // namespace memgraph::tests::simulation

// Required namespace for rapidcheck generators
namespace rc {

using memgraph::tests::simulation::CreateVertex;
using memgraph::tests::simulation::Op;
using memgraph::tests::simulation::OpVariant;
using memgraph::tests::simulation::ScanAll;

template <>
struct Arbitrary<CreateVertex> {
  static Gen<CreateVertex> arbitrary() {
    return gen::build<CreateVertex>(gen::set(&CreateVertex::key, gen::inRange(0, 7)),
                                    gen::set(&CreateVertex::value, gen::inRange(0, 7)));
  }
};

template <>
struct Arbitrary<ScanAll> {
  static Gen<ScanAll> arbitrary() { return gen::just(ScanAll{}); }
};

OpVariant opHoist(ScanAll op) { return op; }
OpVariant opHoist(CreateVertex op) { return op; }

template <>
struct ::rc::Arbitrary<Op> {
  static Gen<Op> arbitrary() {
    return gen::build<Op>(gen::set(
        &Op::inner, gen::oneOf(gen::map(gen::arbitrary<CreateVertex>(), [](CreateVertex op) { return opHoist(op); }),
                               gen::map(gen::arbitrary<ScanAll>(), [](ScanAll op) { return opHoist(op); }))));
  }
};

}  // namespace rc
