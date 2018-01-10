#include <experimental/optional>
#include <sstream>

#include "gtest/gtest.h"

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "utils/serialization.hpp"

using std::experimental::optional;

TEST(SerializationOptionalTest, SerializeAndDeserialize) {
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
