#include "mvcc_find_update_common.hpp"

#undef T2_FIND
#define T2_FIND version_list.find(*t2)

#undef EXPECT_CRE
#undef EXPECT_EXP
#define EXPECT_CRE(record, transaction, command) \
  EXPECT_EQ(record->tx.cre(), id##transaction);  \
  EXPECT_EQ(record->cmd.cre(), command)
#define EXPECT_EXP(record, transaction, command) \
  EXPECT_EQ(record->tx.exp(), id##transaction);  \
  EXPECT_EQ(record->cmd.exp(), command)

// IMPORTANT: look definiton of EXPECT_CRE and EXPECT_EXP macros in
// tests/mvcc_find_update_common.hpp. Numbers in those macros represent
// transaction ids when transactions where created.

TEST_F(Mvcc, UpdateNotAdvanceUpdate) {
  T2_UPDATE;
  EXPECT_EQ(T2_FIND, v1);
  auto v2_2 = version_list.update(*t2);
  EXPECT_NXT(v2, v1);
  EXPECT_EQ(v2, v2_2);
  EXPECT_CRE(v2, 2, 1);
  EXPECT_EXP(v2, 0, 0);
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(2);
}

TEST_F(Mvcc, UpdateNotAdvanceRemove) {
  T2_UPDATE;
  EXPECT_EQ(T2_FIND, v1);
  T2_REMOVE;
  EXPECT_NXT(v2, v1);
  EXPECT_CRE(v2, 2, 1);
  EXPECT_EXP(v2, 0, 0);
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(2);
}

TEST_F(Mvcc, RemoveNotAdvanceUpdate) {
  T2_REMOVE;
  EXPECT_EQ(T2_FIND, v1);
  T2_UPDATE;
  EXPECT_NXT(v2, v1);
  EXPECT_CRE(v2, 2, 1);
  EXPECT_EXP(v2, 0, 0);
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(2);
}

TEST_F(Mvcc, RemoveNotAdvanceRemove) {
  T2_REMOVE;
  EXPECT_EQ(T2_FIND, v1);
  T2_REMOVE;
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(1);
}

TEST_F(Mvcc, UpdateAdvanceUpdate) {
  T2_UPDATE;
  EXPECT_EQ(T2_FIND, v1);
  engine.advance(t2->id);
  EXPECT_EQ(T2_FIND, v2);
  auto v2_2 = version_list.update(*t2);
  EXPECT_NXT(v2, v1);
  EXPECT_NXT(v2_2, v2);
  EXPECT_CRE(v2, 2, 1);
  EXPECT_EXP(v2, 2, 2);
  EXPECT_CRE(v2_2, 2, 2);
  EXPECT_EXP(v2_2, 0, 0);
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(3);
}

TEST_F(Mvcc, UpdateAdvanceRemove) {
  T2_UPDATE;
  EXPECT_EQ(T2_FIND, v1);
  engine.advance(t2->id);
  EXPECT_EQ(T2_FIND, v2);
  T2_REMOVE;
  EXPECT_NXT(v2, v1);
  EXPECT_CRE(v2, 2, 1);
  EXPECT_EXP(v2, 2, 2);
  EXPECT_CRE(v1, 1, 1);
  EXPECT_EXP(v1, 2, 1);
  EXPECT_SIZE(2);
}

TEST_F(Mvcc, RemoveAdvanceUpdate) {
  T2_REMOVE;
  EXPECT_EQ(T2_FIND, v1);
  engine.advance(t2->id);
  EXPECT_EQ(T2_FIND, nullptr);
  EXPECT_DEATH(T2_UPDATE, ".*nullptr.*");
}

TEST_F(Mvcc, RemoveAdvanceRemove) {
  T2_REMOVE;
  EXPECT_EQ(T2_FIND, v1);
  engine.advance(t2->id);
  EXPECT_EQ(T2_FIND, nullptr);
  EXPECT_DEATH(T2_REMOVE, ".*nullptr.*");
}
