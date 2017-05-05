#include "mvcc_find_update_common.hpp"

TEST_F(Mvcc, FindUncommittedHigherTXUpdate) {
  T2_FIND;
  T3_BEGIN;
  T3_UPDATE;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindCommittedHigherTXUpdate) {
  T2_FIND;
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindAbortedHigherTXUpdate) {
  T2_FIND;
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindCommittedLowerTXUpdate) {
  T2_UPDATE;
  T3_BEGIN;
  T3_FIND;
  T2_COMMIT;
  EXPECT_EQ(v3, version_list.find(*t3));
}

TEST_F(Mvcc, FindAbortedLowerTXUpdate) {
  T2_UPDATE;
  T3_BEGIN;
  T3_FIND;
  T2_ABORT;
  EXPECT_EQ(v3, version_list.find(*t3));
}

TEST_F(Mvcc, FindUncommittedHigherTXRemove) {
  T2_FIND;
  T3_BEGIN;
  T3_REMOVE;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindCommittedHigherTXRemove) {
  T2_FIND;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindAbortedHigherTXRemove) {
  T2_FIND;
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, FindCommittedLowerTXRemove) {
  T2_REMOVE;
  T3_BEGIN;
  T3_FIND;
  EXPECT_EQ(v3, version_list.find(*t3));
}

TEST_F(Mvcc, FindAbortedLowerTXRemove) {
  T2_REMOVE;
  T3_BEGIN;
  T3_FIND;
  T2_ABORT;
  EXPECT_EQ(v3, version_list.find(*t3));
}

TEST_F(Mvcc, ReadUncommitedUpdateFromSameTXSameCommand) {
  T2_UPDATE;
  EXPECT_NE(v2, version_list.find(*t2));
}

TEST_F(Mvcc, ReadUncommitedUpdateFromSameTXNotSameCommand) {
  T2_UPDATE;
  engine.advance(t2->id);
  EXPECT_EQ(v2, version_list.find(*t2));
}

TEST_F(Mvcc, ReadUncommitedRemoveFromSameTXSameCommand) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_REMOVE;
  EXPECT_EQ(v2, version_list.find(*t3));
}

TEST_F(Mvcc, ReadUncommitedRemoveFromSameTXNotSameCommand) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_REMOVE;
  engine.advance(t3->id);
  EXPECT_NE(v2, version_list.find(*t3));
}
