// tests in this suite deal with edge cases in logical operator behavior
// that's not easily testable with single-phase testing. instead, for
// easy testing and latter readability they are tested end-to-end.

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "query/interpreter.hpp"

class QueryExecution : public testing::Test {
 protected:
  Dbms dbms_;
  std::unique_ptr<GraphDbAccessor> db_ = dbms_.active();

  /** Commits the current transaction and refreshes the db_
   * variable to hold a new accessor with a new transaction */
  void Commit() {
    db_->commit();
    auto next_db = dbms_.active();
    db_.swap(next_db);
  }

  /** Executes the query and returns the results.
   * Does NOT commit the transaction */
  auto Execute(const std::string &query) {
    ResultStreamFaker results;
    query::Interpreter().Interpret(query, *db_, results, {});
    return results.GetResults();
  }
};

TEST_F(QueryExecution, MissingOptionalIntoExpand) {
  // validating bug where expanding from Null (due to a preceeding optional
  // match) exhausts the expansion cursor, even if it's input is still not
  // exhausted
  Execute(
      "CREATE (a:Person {id: 1}), (b:Person "
      "{id:2})-[:Has]->(:Dog)-[:Likes]->(:Food )");
  Commit();
  ASSERT_EQ(Execute("MATCH (n) RETURN n").size(), 4);

  auto Exec = [this](bool desc, bool variable) {
    // this test depends on left-to-right query planning
    FLAGS_query_cost_planner = false;
    return Execute(std::string("MATCH (p:Person) WITH p ORDER BY p.id ") +
                   (desc ? "DESC " : "") +
                   "OPTIONAL MATCH (p)-->(d:Dog) WITH p, d "
                   "MATCH (d)-" + (variable ? "[*1]" : "") + "->(f:Food) "
                   "RETURN p, d, f")
        .size();
  };

  EXPECT_EQ(Exec(false, false), 1);
  EXPECT_EQ(Exec(true, false), 1);
  EXPECT_EQ(Exec(false, true), 1);
  EXPECT_EQ(Exec(true, true), 1);

  // TODO test/fix ExpandBreadthFirst once it's operator lands
}
