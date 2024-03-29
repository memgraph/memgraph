// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/exceptions.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_string(use_db, "memgraph", "Database to run the query against");

using namespace memgraph::communication::bolt;

class BoltClient : public ::testing::Test {
 protected:
  virtual void SetUp() {
    client_.Connect(endpoint_, FLAGS_username, FLAGS_password);
    Execute("CREATE DATABASE db1");
  }

  virtual void TearDown() {}

  bool Execute(const std::string &query, const std::string &message = "") {
    try {
      auto ret = client_.Execute(query, {});
    } catch (const ClientQueryException &e) {
      if (message != "") {
        EXPECT_EQ(e.what(), message);
      }
      throw;
    }
    return true;
  }

  bool ExecuteAndCheckQid(const std::string &query, int qid, const std::string &message = "") {
    try {
      auto ret = client_.Execute(query, {});
      if (ret.metadata["qid"].ValueInt() != qid) {
        return false;
      }
    } catch (const ClientQueryException &e) {
      if (message != "") {
        EXPECT_EQ(e.what(), message);
      }
      throw;
    }
    return true;
  }

  int64_t GetCount() {
    auto ret = client_.Execute("match (n) return count(n)", {});
    EXPECT_EQ(ret.records.size(), 1);
    EXPECT_EQ(ret.records[0].size(), 1);
    EXPECT_TRUE(ret.records[0][0].IsInt());
    return ret.records[0][0].ValueInt();
  }

  bool TransactionActive() {
    try {
      client_.Execute("begin", {});
    } catch (const ClientQueryException &e) {
      return true;
    }
    return false;
  }

  memgraph::io::network::Endpoint endpoint_{memgraph::io::network::ResolveHostname(FLAGS_address),
                                            static_cast<uint16_t>(FLAGS_port)};
  memgraph::communication::ClientContext context_{FLAGS_use_ssl};
  Client client_{context_};
};

const std::string kNoCurrentTransactionToCommit = "No current transaction to commit.";
const std::string kNoCurrentTransactionToRollback = "No current transaction to rollback.";
const std::string kNestedTransactions = "Nested transactions are not supported.";
const std::string kCommitInvalid =
    "Transaction can't be committed because there was a previous error. Please "
    "invoke a rollback instead.";

TEST_F(BoltClient, SelectDB) { Execute(fmt::format("USE DATABASE {}", FLAGS_use_db)); }

TEST_F(BoltClient, SelectDBUnderTx) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("USE DATABASE memgraph", "Multi-database queries are not allowed in multicommand transactions."),
               ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CommitWithoutTransaction) {
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, RollbackWithoutTransaction) {
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, DoubleCommitWithoutTransaction) {
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, DoubleRollbackWithoutTransaction) {
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, DoubleBegin) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, DoubleBeginAndCommit) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, DoubleBeginAndRollback) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndDoubleCommit) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("commit"));
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndDoubleRollback) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndCorrectQueriesAndCommit) {
  EXPECT_TRUE(Execute("begin"));
  auto count = GetCount();
  EXPECT_TRUE(Execute("create (n)"));
  ASSERT_EQ(GetCount(), count + 1);
  EXPECT_TRUE(Execute("commit"));
  EXPECT_EQ(GetCount(), count + 1);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndCorrectQueriesAndRollback) {
  EXPECT_TRUE(Execute("begin"));
  auto count = GetCount();
  EXPECT_TRUE(Execute("create (n)"));
  ASSERT_EQ(GetCount(), count + 1);
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_EQ(GetCount(), count);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndCorrectQueriesAndBegin) {
  EXPECT_TRUE(Execute("begin"));
  auto count = GetCount();
  EXPECT_TRUE(Execute("create (n)"));
  ASSERT_EQ(GetCount(), count + 1);
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_EQ(GetCount(), count);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndWrongQueryAndRollback) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndWrongQueryAndCommit) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, BeginAndWrongQueryAndBegin) {
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(TransactionActive());
}

TEST_F(BoltClient, CommitAndCorrectQueryAndCommit) {
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_TRUE(Execute("create (n)"));
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CommitAndWrongQueryAndCommit) {
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, RollbackAndCorrectQueryAndRollback) {
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_TRUE(Execute("create (n)"));
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, RollbackAndWrongQueryAndRollback) {
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueryAndBeginAndCommit) {
  EXPECT_TRUE(Execute("match (n) return count(n)"));
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueryAndBeginAndRollback) {
  EXPECT_TRUE(Execute("match (n) return count(n)"));
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueryAndBeginAndBegin) {
  EXPECT_TRUE(Execute("match (n) return count(n)"));
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueryAndBeginAndCommit) {
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueryAndBeginAndRollback) {
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueryAndBeginAndBegin) {
  EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndCorrectQueriesAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndCorrectQueriesAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndCorrectQueriesAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndCorrectQueriesAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndCorrectQueriesAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("rollback"));
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndCorrectQueriesAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_THROW(Execute("begin", kNestedTransactions), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndWrongQueriesAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndWrongQueriesAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, CorrectQueriesAndBeginAndWrongQueriesAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndWrongQueriesAndCommit) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_THROW(Execute("commit", kNoCurrentTransactionToCommit), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndWrongQueriesAndRollback) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_THROW(Execute("rollback", kNoCurrentTransactionToRollback), ClientQueryException);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, WrongQueriesAndBeginAndWrongQueriesAndBegin) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_THROW(Execute("asdasd"), ClientQueryException);
  }
  EXPECT_TRUE(Execute("begin"));
  EXPECT_TRUE(TransactionActive());
}

TEST_F(BoltClient, MixedCaseAndWhitespace) {
  EXPECT_TRUE(Execute("   bEgiN   \n\n"));
  auto count = GetCount();
  EXPECT_TRUE(Execute("   cReATe     (   n   )   \n\n"));
  ASSERT_EQ(GetCount(), count + 1);
  EXPECT_TRUE(Execute("    COMmit  "));
  EXPECT_EQ(GetCount(), count + 1);
  EXPECT_FALSE(TransactionActive());
}

TEST_F(BoltClient, TestQid) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(Execute("match (n) return count(n)"));
  }
  EXPECT_TRUE(Execute("begin"));
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(ExecuteAndCheckQid("match (n) return count(n)", i + 1));
  }
  EXPECT_TRUE(Execute("commit"));
  EXPECT_FALSE(TransactionActive());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  return RUN_ALL_TESTS();
}
