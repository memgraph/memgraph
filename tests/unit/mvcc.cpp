#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

class Prop : public mvcc::Record<Prop> {};

TEST(MVCC, Case1Test3) {
  tx::Engine engine;
  auto t1 = engine.begin();
  Prop *prop;
  mvcc::VersionList<Prop> version_list(*t1, prop);
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
  Prop *prop;
  auto t1 = engine.begin();
  mvcc::VersionList<Prop> version_list(*t1, prop);
  t1->commit();

  auto t2 = engine.begin();
  version_list.update(*t2);
  auto t3 = engine.begin();  // t2 is in snapshot of t3
  auto v = version_list.find(*t3);
  t2->commit();

  EXPECT_THROW(version_list.update(v, *t3), SerializationError);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
