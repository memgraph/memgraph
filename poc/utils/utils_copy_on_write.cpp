#include "gtest/gtest.h"

#include "utils/copy_on_write.hpp"

TEST(CopyOneWrite, Dereferencing) {
  utils::CopyOnWrite<std::string> number("42");
  EXPECT_EQ((*number).size(), 2);
  EXPECT_EQ(number.get().size(), 2);
  EXPECT_EQ(number->size(), 2);
}

TEST(CopyOneWrite, Ownership) {
  utils::CopyOnWrite<std::string> number("42");
  EXPECT_TRUE(number.is_owner());

  utils::CopyOnWrite<std::string> copy_constructed(number);
  auto copy_assigned = number;
  EXPECT_FALSE(copy_constructed.is_owner());
  EXPECT_FALSE(copy_assigned.is_owner());
  EXPECT_TRUE(number.is_owner());
}

TEST(CopyOneWrite, OwnershipFromTemporary) {
  utils::CopyOnWrite<std::string> copy_constructed(
      utils::CopyOnWrite<std::string>("42"));
  auto copy_assigned = utils::CopyOnWrite<std::string>("42");
  EXPECT_TRUE(copy_constructed.is_owner());
  EXPECT_TRUE(copy_assigned.is_owner());
}

struct DestructorCounter {
  DestructorCounter(int &counter) : counter_(counter) {}
  ~DestructorCounter() {
    counter_++;
  }
  private:
    int &counter_;
};

TEST(CopyOnWrite, ElementDestruction) {
  int counter = 0;
  std::vector<utils::CopyOnWrite<DestructorCounter>> initial_owner;
  initial_owner.emplace_back(counter);
  {
    auto copy = initial_owner[0];
    EXPECT_EQ(counter, 0);
    initial_owner.clear();
    EXPECT_EQ(counter, 0);
  }
  EXPECT_EQ(counter, 1);
}

TEST(CopyOneWrite, Copy) {
  utils::CopyOnWrite<std::string> number("42");
  auto copy = number;
  EXPECT_EQ(*copy, "42");

  // modification by owner
  number.Write().resize(1);
  EXPECT_EQ(*number, "4");
  EXPECT_EQ(*copy, "4");

  // modification by copy
  copy.Write().resize(2, '3');
  EXPECT_EQ(*number, "4");
  EXPECT_EQ(*copy, "43");
}
