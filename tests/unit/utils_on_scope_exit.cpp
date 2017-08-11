#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/on_scope_exit.hpp"


TEST(OnScopeExit, BasicUsage) {
  int variable = 1;
  {
    ASSERT_EQ(variable, 1);
    utils::OnScopeExit on_exit([&variable] { variable = 2; });
    EXPECT_EQ(variable, 1);
  }
  EXPECT_EQ(variable, 2);
}
