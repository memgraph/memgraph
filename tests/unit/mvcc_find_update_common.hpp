#include <vector>
#include "gtest/gtest.h"

#include "mvcc/id.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version.hpp"
#include "mvcc/version_list.hpp"
#include "threading/sync/lock_timeout_exception.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.cpp"

// make it easy to compare Id with int
bool operator==(const Id &left, const int right) {
  return static_cast<uint64_t>(left) == static_cast<uint64_t>(right);
}

class TestClass : public mvcc::Record<TestClass> {
  friend std::ostream &operator<<(std::ostream &stream, TestClass &test_class) {
    stream << test_class.tx.cre() << " " << test_class.tx.exp();
    return stream;
  }
};

/**
 * Testing mvcc::VersionList::find behavior in
 * different situations (preceeding update/remove ops
 * in different transactions).
 *
 * The setup for each case is:
 *  - transaction t1 has created a new version_list v1 and commited
 *  - transaction t2 has strated
 *  - *********************
 *  - here the test fixture ends and custom test behavior should be added
 *  - *********************
 *  - tests should check every legal sequence of the following ops
 *      - creation of transaction t3
 *      - [commit/abort] of [t2/t3]
 *      - [removal/update] on version_list by [t2/t3]
 *  - illegal sequences (for example double commit) don't have to be checked
 */
class Mvcc : public ::testing::Test {
 protected:
  virtual void SetUp() {
    t1 = &engine.advance(t1->id);
    v1 = version_list.find(*t1);
    t1->commit();
    t2 = engine.begin();
  }

  tx::Engine engine;
  tx::Transaction *t1 = engine.begin();
  mvcc::VersionList<TestClass> version_list{*t1};
  TestClass *v1 = nullptr;
  tx::Transaction *t2 = nullptr;
};

// helper macros. important:
//  - TX_FIND and TX_UPDATE set the record variable vX
//  - TX_BEGIN sets the transaction variable tX
#define T2_FIND __attribute__((unused)) auto v2 = version_list.find(*t2)
#define T3_FIND __attribute__((unused)) auto v3 = version_list.find(*t3)
#define T2_UPDATE __attribute__((unused)) auto v2 = version_list.update(*t2)
#define T3_UPDATE __attribute__((unused)) auto v3 = version_list.update(*t3)
#define T2_COMMIT t2->commit();
#define T3_COMMIT t3->commit();
#define T2_ABORT t2->abort();
#define T3_ABORT t3->abort();
#define T3_BEGIN auto t3 = engine.begin();
#define T2_REMOVE version_list.remove(*t2)
#define T3_REMOVE version_list.remove(*t3)
#define EXPECT_CRE(record, expected) EXPECT_EQ(record->tx.cre(), expected)
#define EXPECT_EXP(record, expected) EXPECT_EQ(record->tx.exp(), expected)

// test the fixture
TEST_F(Mvcc, Fixture) {
  EXPECT_CRE(v1, 1);
  EXPECT_EXP(v1, 0);
}
