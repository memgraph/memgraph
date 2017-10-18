#include "mvcc_find_update_common.hpp"

#undef T4_FIND
#define T4_FIND version_list.find(*t4)
#undef T3_FIND
#define T3_FIND version_list.find(*t3)

// IMPORTANT: look definiton of EXPECT_CRE and EXPECT_EXP macros in
// tests/mvcc_find_update_common.hpp. Numbers in those macros represent
// transaction ids when transactions where created.

// ****************************************************************
// *   CASE 1: T3 starts after T2 ends.
// *
// * T2:   START---OP---END
// *
// * T3:                      START---OP---END
// *
// * T4:                                        START---FIND---END
// ****************************************************************

TEST_F(Mvcc, UpdCmtUpdCmt1) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v2);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v3);
}

TEST_F(Mvcc, UpdCmtRemCmt1) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 3);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, RemCmtUpdCmt1) {
  T2_REMOVE;
  T2_COMMIT;
  T3_BEGIN;
  EXPECT_DEATH(T3_UPDATE, ".*nullptr.*");
}

TEST_F(Mvcc, RemCmtRemCmt1) {
  T2_REMOVE;
  T2_COMMIT;
  T3_BEGIN;
  EXPECT_FALSE(T3_FIND);
}

TEST_F(Mvcc, UpdCmtUpdAbt1) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v2, v1);
  EXPECT_NXT(v3, v2);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v2);
}

TEST_F(Mvcc, UpdCmtRemAbt1) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 3);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v2);
}

TEST_F(Mvcc, RemCmtUpdAbt1) {
  T2_REMOVE;
  T2_COMMIT;
  T3_BEGIN;
  EXPECT_DEATH(T3_UPDATE, ".*nullptr.*");
}

TEST_F(Mvcc, RemCmtRemAbt1) {
  T2_REMOVE;
  T2_COMMIT;
  T3_BEGIN;
  EXPECT_FALSE(T3_FIND);
}

TEST_F(Mvcc, UpdAbtUpdCmt1) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v2);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v3);
}

TEST_F(Mvcc, UpdAbtRemCmt1) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_NXT(v2, v1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, RemAbtUpdCmt1) {
  T2_REMOVE;
  T2_ABORT;
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v3);
}

TEST_F(Mvcc, RemAbtRemCmt1) {
  T2_REMOVE;
  T2_ABORT;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, UpdAbtUpdAbt1) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v2, v1);
  EXPECT_NXT(v3, v2);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, UpdAbtRemAbt1) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_NXT(v2, v1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtUpdAbt1) {
  T2_REMOVE;
  T2_ABORT;
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtRemAbt1) {
  T2_REMOVE;
  T2_ABORT;
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

// ****************************************************************
// *   CASE 2: T3 starts before T2 ends.
// *
// * T2:   START---OP---END
// *
// * T3:         START---------OP---END
// *
// * T4:                                  START---FIND---END
// *
// ****************************************************************

// ****************************
// COVERS 8 cases!
TEST_F(Mvcc, UpdCmtUpd2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_COMMIT;
  EXPECT_THROW(T3_UPDATE, mvcc::SerializationError);
}

TEST_F(Mvcc, UpdCmtRem2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_COMMIT;
  EXPECT_THROW(T3_REMOVE, mvcc::SerializationError);
}

TEST_F(Mvcc, RemCmtUpd2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_COMMIT;
  EXPECT_THROW(T3_UPDATE, mvcc::SerializationError);
}

TEST_F(Mvcc, RemCmtRem2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_COMMIT;
  EXPECT_THROW(T3_REMOVE, mvcc::SerializationError);
}
// **************************

TEST_F(Mvcc, UpdAbtUpdCmt2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_ABORT;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v2);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v3);
}

TEST_F(Mvcc, UpdAbtRemCmt2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_ABORT;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_NXT(v2, v1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, RemAbtUpdCmt2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_ABORT;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v3);
}

TEST_F(Mvcc, RemAbtRemCmt2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_ABORT;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_EXP(v1, 3);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, UpdAbtUpdAbt2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_ABORT;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v2, 0);
  EXPECT_NXT(v2, v1);
  EXPECT_NXT(v3, v2);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, UpdAbtRemAbt2) {
  T2_UPDATE;
  T3_BEGIN;
  T2_ABORT;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_NXT(v2, v1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtUpdAbt2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_ABORT;
  T3_UPDATE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtRemAbt2) {
  T2_REMOVE;
  T3_BEGIN;
  T2_ABORT;
  T3_REMOVE;
  T3_ABORT;
  EXPECT_EXP(v1, 3);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

// ****************************************************************
// *   CASE 3: T3 ends before T2 starts executing operations.
// *
// * T2:   START--------------------OP---END
// *
// * T3:         START---OP---END
// *
// * T4:                                        START---FIND---END
// *
// ****************************************************************

// ****************************
// COVERS 8 cases!
TEST_F(Mvcc, UpdUpdCmt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_THROW(T2_UPDATE, mvcc::SerializationError);
}

TEST_F(Mvcc, UpdRemCmt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_THROW(T2_UPDATE, mvcc::SerializationError);
}

TEST_F(Mvcc, RemUpdCmt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_THROW(T2_REMOVE, mvcc::SerializationError);
}

TEST_F(Mvcc, RemRemCmt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_THROW(T2_REMOVE, mvcc::SerializationError);
}
// **************************

TEST_F(Mvcc, UpdCmtUpdAbt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_UPDATE;
  T2_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_NXT(v2, v3);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v2);
}

TEST_F(Mvcc, UpdCmtRemAbt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  T2_UPDATE;
  T2_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_NXT(v2, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v2);
}

TEST_F(Mvcc, RemCmtUpdAbt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_REMOVE;
  T2_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, RemCmtRemAbt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  T2_REMOVE;
  T2_COMMIT;
  EXPECT_EXP(v1, 2);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, nullptr);
}

TEST_F(Mvcc, UpdAbtUpdAbt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_UPDATE;
  T2_ABORT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_NXT(v2, v3);
  EXPECT_SIZE(3);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, UpdAbtRemAbt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  T2_UPDATE;
  T2_ABORT;
  EXPECT_NXT(v2, v1);
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtUpdAbt3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_REMOVE;
  T2_ABORT;
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_NXT(v3, v1);
  EXPECT_SIZE(2);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

TEST_F(Mvcc, RemAbtRemAbt3) {
  T3_BEGIN;
  T3_REMOVE;
  T3_ABORT;
  T2_REMOVE;
  T2_ABORT;
  EXPECT_EXP(v1, 2);
  EXPECT_SIZE(1);
  T4_BEGIN;
  EXPECT_EQ(T4_FIND, v1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
