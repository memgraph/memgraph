#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

class Prop : public mvcc::Record<Prop> {};
class PropCount : public mvcc::Record<PropCount> {
 public:
  PropCount(std::atomic<int> &count) : count_(count) {}
  ~PropCount() { ++count_; }

 private:
  std::atomic<int> &count_;
};

TEST(MVCC, Case1Test3) {
  tx::Engine engine;
  auto t1 = engine.begin();
  mvcc::VersionList<Prop> version_list(*t1);
  t1->commit();

  auto t2 = engine.begin();
  version_list.update(*t2);
  t2->commit();

  auto t3 = engine.begin();
  auto t4 = engine.begin();
  version_list.update(*t4);
  t4->commit();
  EXPECT_THROW(version_list.remove(*t3), SerializationError);
}

TEST(MVCC, InSnapshotSerializationError) {
  tx::Engine engine;
  auto t1 = engine.begin();
  mvcc::VersionList<Prop> version_list(*t1);
  t1->commit();

  auto t2 = engine.begin();
  version_list.update(*t2);
  auto t3 = engine.begin();  // t2 is in snapshot of t3
  t2->commit();

  EXPECT_THROW(version_list.update(*t3), SerializationError);
}

// Check that we don't delete records when we re-link.
TEST(MVCC, UpdateDontDelete) {
  std::atomic<int> count{0};
  {
    tx::Engine engine;
    auto t1 = engine.begin();
    mvcc::VersionList<PropCount> version_list(*t1, count);
    t1->commit();

    auto t2 = engine.begin();
    version_list.update(*t2);
    t2->abort();
    EXPECT_EQ(count, 0);

    auto t3 = engine.begin();

    // Update re-links the node and shouldn't clear it yet.
    version_list.update(*t3);
    EXPECT_EQ(count, 0);

    t3->commit();
  }
  EXPECT_EQ(count, 3);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
