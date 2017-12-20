#include <sstream>

#include "cereal/archives/json.hpp"
#include "gtest/gtest.h"

#include "utils/cereal_optional.hpp"

using std::experimental::optional;

TEST(CerealOptionalTest, SerializeAndDeserialize) {
  std::stringstream ss;

  optional<int> x1 = {};
  optional<int> x2 = 42;
  optional<int> y1, y2;

  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(x1, x2);
  }

  {
    cereal::JSONInputArchive iarchive(ss);
    iarchive(y1, y2);
  }

  EXPECT_EQ(x1, y1);
  EXPECT_EQ(x2, y2);
}
