#include "gtest/gtest.h"

#include <vector>

#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

TEST(Engine, Count) {
  tx::Engine eng;
  EXPECT_EQ(eng.count(), 0);
}

TEST(Engine, CountFive) {
  tx::Engine eng;
  EXPECT_EQ(eng.count(), (uint64_t)0);
  std::vector<tx::Transaction *> V;
  for (int i = 0; i < 5; ++i) {
    V.push_back(eng.begin());
    EXPECT_EQ(eng.count(), (uint64_t)(i + 1));
  }
  EXPECT_EQ(eng.size(), (uint64_t)5);
  for (int i = 0; i < 5; ++i) V[i]->commit();
  EXPECT_EQ(eng.count(), (uint64_t)5);
}

TEST(Engine, LastKnownActiveEmpty) {
  tx::Engine eng;
  EXPECT_EQ(eng.oldest_active().is_present(), false);
}

TEST(Engine, LastKnownActive) {
  tx::Engine eng;
  std::vector<tx::Transaction *> V;
  for (int i = 0; i < 5; ++i) {
    V.push_back(eng.begin());
    EXPECT_EQ(eng.size(), (size_t)i + 1);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(eng.oldest_active().get(), Id(i + 1));
    V[i]->commit();
  }
  EXPECT_EQ(eng.oldest_active().is_present(), false);
}

TEST(Engine, Size) {
  tx::Engine eng;
  std::vector<tx::Transaction *> V;
  for (int i = 0; i < 5; ++i) {
    V.push_back(eng.begin());
    EXPECT_EQ(eng.size(), (size_t)i + 1);
  }
  for (int i = 0; i < 5; ++i) V[i]->commit();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
