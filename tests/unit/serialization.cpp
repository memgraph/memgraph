#include <experimental/optional>
#include <sstream>

#include "gtest/gtest.h"

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "utils/serialization.hpp"

using std::experimental::optional;
using std::string_literals::operator""s;

TEST(Serialization, Optional) {
  std::stringstream ss;

  optional<int> x1 = {};
  optional<int> x2 = 42;
  optional<int> y1, y2;

  {
    boost::archive::binary_oarchive ar(ss);
    ar << x1;
    ar << x2;
  }

  {
    boost::archive::binary_iarchive ar(ss);
    ar >> y1;
    ar >> y2;
  }

  EXPECT_EQ(x1, y1);
  EXPECT_EQ(x2, y2);
}

TEST(Serialization, Tuple) {
  std::stringstream ss;

  auto x1 = std::make_tuple("foo"s, 42, std::experimental::make_optional(3.14));
  auto x2 = std::make_tuple();
  auto x3 = std::make_tuple(1, 2, 3, 4, 5);

  decltype(x1) y1;
  decltype(x2) y2;
  decltype(x3) y3;

  {
    boost::archive::binary_oarchive ar(ss);
    ar << x1;
    ar << x2;
    ar << x3;
  }

  {
    boost::archive::binary_iarchive ar(ss);
    ar >> y1;
    ar >> y2;
    ar >> y3;
  }

  EXPECT_EQ(x1, y1);
  EXPECT_EQ(x2, y2);
  EXPECT_EQ(x3, y3);
}
