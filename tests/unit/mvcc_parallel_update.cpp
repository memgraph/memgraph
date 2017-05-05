#include "mvcc_find_update_common.hpp"

// TODO Gradicek: rename all existing cases
// TODO Gradicek: check validity of all existing cases
// TODO Gradicek: add all other cases (48 in total, discuss with Flor)
// TODO Gradicek: what about command advance testing,
//                as opposed to transaction commit/abort?

TEST_F(Mvcc, Case1_InsertWithUpdates) {
  T2_UPDATE;
  T2_COMMIT;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;
  EXPECT_EXP(v2, 3);
}

TEST_F(Mvcc, RemoveUpdatedRecord) {
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_THROW(T2_REMOVE, mvcc::SerializationError);
}

TEST_F(Mvcc, UpdateUpdatedRecord) {
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_THROW(version_list.update(*t2), mvcc::SerializationError);
}

TEST_F(Mvcc, Case2_AbortUpdate_Remove_T10) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_REMOVE;
  T3_COMMIT;

  EXPECT_CRE(v1, 1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
}

TEST_F(Mvcc, Case2_AbortUpdate_Remove_T7) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_REMOVE;
  T2_COMMIT;
  EXPECT_CRE(v1, 1);
  EXPECT_EXP(v1, 2);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
}

TEST_F(Mvcc, Case2_AbortUpdate_Update_T10) {
  T2_UPDATE;
  T2_ABORT;
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;

  EXPECT_CRE(v1, 1);
  EXPECT_EXP(v1, 3);
  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
}

TEST_F(Mvcc, Case2_AbortUpdate_Update_T7) {
  T3_BEGIN;
  T3_UPDATE;
  T3_ABORT;
  T2_UPDATE;
  T2_COMMIT;

  EXPECT_CRE(v3, 3);
  EXPECT_EXP(v3, 0);
  EXPECT_CRE(v2, 2);
  EXPECT_EXP(v2, 0);
}

TEST_F(Mvcc, Case1Test3) {
  T3_BEGIN;
  T3_UPDATE;
  T3_COMMIT;
  EXPECT_THROW(T2_REMOVE, mvcc::SerializationError);
}
