// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include <memory>

#include "utils/observer.hpp"

namespace {
class TestObservable : public memgraph::utils::Observable<TestObservable> {
 public:
  void Accept(std::shared_ptr<memgraph::utils::Observer<TestObservable>> obs) { obs->Update(*this); }

  void Modify(std::string_view s_in) {
    s = s_in;
    Notify();
  }

  void Modify(int i_in) {
    i = i_in;
    Notify();
  }

  std::string s{};
  int i{};
};

class TestObserver : public memgraph::utils::Observer<TestObservable> {
 public:
  void Update(const TestObservable &in) {
    last_s = in.s;
    last_i = in.i;
  }
  std::string last_s{};
  int last_i{};
};

}  // namespace

TEST(Observer, BasicFunctionality) {
  TestObservable observable;

  auto observer1 = std::make_shared<TestObserver>();
  auto observer2 = std::make_shared<TestObserver>();
  auto observer3 = std::make_shared<TestObserver>();

  // No observers
  observable.Modify("abc");
  observable.Modify(123);

  // Attach observers
  observable.Attach(observer1);
  observable.Attach(observer2);
  observable.Modify(45);
  observable.Modify("qwe");
  EXPECT_EQ(observer1->last_s, "qwe");
  EXPECT_EQ(observer1->last_i, 45);
  EXPECT_EQ(observer2->last_s, "qwe");
  EXPECT_EQ(observer2->last_i, 45);

  // Detach one observer
  observable.Detach(observer1);
  observable.Modify(678);
  observable.Modify("iop");
  EXPECT_EQ(observer1->last_s, "qwe");
  EXPECT_EQ(observer1->last_i, 45);
  EXPECT_EQ(observer2->last_s, "iop");
  EXPECT_EQ(observer2->last_i, 678);

  // Add a 3rd observer and re-attach
  observable.Attach(observer3);
  observable.Attach(observer1);
  observable.Modify(890);
  observable.Modify("bnm");
  EXPECT_EQ(observer1->last_s, "bnm");
  EXPECT_EQ(observer1->last_i, 890);
  EXPECT_EQ(observer2->last_s, "bnm");
  EXPECT_EQ(observer2->last_i, 890);
  EXPECT_EQ(observer3->last_s, "bnm");
  EXPECT_EQ(observer3->last_i, 890);
}
