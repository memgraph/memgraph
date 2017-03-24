#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

class Prop : public mvcc::Record<Prop> {};

TEST(MVCC, Case1Test3) {
  tx::Engine engine;
  mvcc::VersionList<Prop> version_list;
  auto t1 = engine.begin();
  version_list.insert(*t1);
  t1->commit();

  auto t2 = engine.begin();
  auto v2 = version_list.update(*t2);
  t2->commit();

  auto t3 = engine.begin();
  auto t4 = engine.begin();
  version_list.update(*t4);
  t4->commit();
  EXPECT_THROW(version_list.remove(v2, *t3), SerializationError);
}

TEST(MVCC, InSnapshot) {
  tx::Engine engine;
  mvcc::VersionList<Prop> version_list;
  auto t1 = engine.begin();
  auto v1 = version_list.insert(*t1);
  version_list.update(*t1);  // expire old record and create new
  auto t2 = engine.begin();  // t1 is in snapshot of t2
  t1->commit();

  EXPECT_THROW(version_list.update(v1, *t2), SerializationError);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
