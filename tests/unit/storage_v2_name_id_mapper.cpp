#include <gtest/gtest.h>

#include "storage/v2/name_id_mapper.hpp"

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(NameIdMapper, Basic) {
  storage::NameIdMapper mapper;

  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.NameToId("n3"), 2);

  ASSERT_EQ(mapper.IdToName(0), "n1");
  ASSERT_EQ(mapper.IdToName(1), "n2");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(NameIdMapper, Correctness) {
  storage::NameIdMapper mapper;

  ASSERT_DEATH(mapper.IdToName(0), "");
  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.IdToName(0), "n1");

  ASSERT_DEATH(mapper.IdToName(1), "");
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.IdToName(1), "n2");

  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);

  ASSERT_EQ(mapper.IdToName(1), "n2");
  ASSERT_EQ(mapper.IdToName(0), "n1");
}
