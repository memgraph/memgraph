// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "gtest/gtest.h"

#include "utils/logging.hpp"

namespace {
// The text as it would be logged: the original when nothing was redacted.
std::string Logged(std::string_view const query) {
  auto masked = memgraph::logging::MaskSensitiveInformation(query);
  return masked ? *std::move(masked) : std::string{query};
}
}  // namespace

TEST(MaskPasswords, AwsKeys) {
  EXPECT_EQ(
      Logged("LOAD PARQUET FROM 's3://deps.memgraph.io/nodes_100.parquet' WITH CONFIG {'aws_region': "
             "'random_region', 'aws_access_key': 'test', 'aws_secret_key': 'test1'} AS row CREATE (n:N {id: row.id, "
             "name: row.name, age: row.age, city: row.city});"),
      "LOAD PARQUET FROM 's3://deps.memgraph.io/nodes_100.parquet' WITH CONFIG {'aws_region': 'random_region', "
      "'aws_access_key': '****', 'aws_secret_key': '****'} AS row CREATE (n:N {id: row.id, name: row.name, age: "
      "row.age, city: row.city});");

  EXPECT_EQ(Logged("set database setting 'aws.access_key' to 'test'"),
            "set database setting 'aws.access_key' to '****'");
  EXPECT_EQ(Logged("set database setting 'aws.secret_key' to 'test'"),
            "set database setting 'aws.secret_key' to '****'");
}

TEST(MaskPasswords, GeneralCases) {
  EXPECT_EQ(Logged("CALL migrate.sql_server('example_table', {user:'memgraph', password:'password', host:'localhost', "
                   "database:'demo_db'} ) YIELD row RETURN row;"),
            "CALL migrate.sql_server('example_table', {user:'memgraph', password:'****', host:'localhost', "
            "database:'demo_db'} ) YIELD row RETURN row;");

  EXPECT_EQ(
      Logged("CALL migrate.sql_server('example_table', {user:'memgraph', password:\"password\", host:'localhost', "
             "database:'demo_db'} ) YIELD row RETURN row;"),
      "CALL migrate.sql_server('example_table', {user:'memgraph', password:\"****\", host:'localhost', "
      "database:'demo_db'} ) YIELD row RETURN row;");

  EXPECT_EQ(Logged("SET PASSWORD TO 'newpassword' REPLACE 'oldpassword'"), "SET PASSWORD TO '****' REPLACE '****'");

  EXPECT_EQ(Logged("SET PASSWORD TO \"newpassword\" REPLACE \"oldpassword\""),
            "SET PASSWORD TO \"****\" REPLACE \"****\"");

  EXPECT_EQ(Logged("CREATE USER `alice@memgraph.com` IDENTIFIED BY '0042';"),
            "CREATE USER `alice@memgraph.com` IDENTIFIED BY '****';");

  EXPECT_EQ(Logged("SET PASSWORD FOR user_name TO 'new_password';"), "SET PASSWORD FOR user_name TO '****';");

  EXPECT_EQ(Logged("SET PASSWORD FOR user_name TO \"new_password\";"), "SET PASSWORD FOR user_name TO \"****\";");

  EXPECT_EQ(Logged("SET PASWORD FOR user_name TO 'new_password';"), "SET PASWORD FOR user_name TO '****';");

  EXPECT_EQ(Logged("SET PASSWORD TO 'newpassword' RPLACE 'oldpassword'"), "SET PASSWORD TO '****' RPLACE '****'");
}

TEST(MaskPasswords, NodePropertyCases) {
  EXPECT_EQ(
      Logged("CREATE (g1:G {name: 'g1', password: 'password'}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
             "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');"),
      "CREATE (g1:G {name: 'g1', password: '****'}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
      "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');");

  EXPECT_EQ(
      Logged(
          "CREATE (g1:G {name: 'g1', password: \"password\"}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
          "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');"),
      "CREATE (g1:G {name: 'g1', password: \"****\"}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
      "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');");
}

TEST(MaskPasswords, TextWithoutACredentialIsNotCopied) {
  EXPECT_FALSE(memgraph::logging::MaskSensitiveInformation("MATCH (a), (b) CREATE (a)-[:Type]->(b)").has_value());
  EXPECT_FALSE(memgraph::logging::MaskSensitiveInformation("MATCH (n) WHERE n.name = 'Alice' RETURN n").has_value());
  EXPECT_FALSE(memgraph::logging::MaskSensitiveInformation("").has_value());
}

TEST(MaskPasswords, ValueQuoteDoesNotEndTheRedaction) {
  EXPECT_EQ(Logged(R"(CREATE USER u IDENTIFIED BY 'pa\'ss';)"), R"(CREATE USER u IDENTIFIED BY '****';)");

  EXPECT_EQ(Logged(R"(SET PASSWORD TO 'ab"cd';)"), R"(SET PASSWORD TO '****';)");

  EXPECT_EQ(Logged(R"(SET PASSWORD TO "ab'cd";)"), R"(SET PASSWORD TO "****";)");
}

TEST(MaskPasswords, UnterminatedValueIsRedactedToTheEnd) {
  EXPECT_EQ(Logged("SET PASSWORD TO 'hunter2"), "SET PASSWORD TO '****");

  EXPECT_EQ(Logged("CREATE USER u IDENTIFIED BY 'hunter2"), "CREATE USER u IDENTIFIED BY '****");

  // A trailing backslash escapes nothing, so the value still has no closing quote.
  EXPECT_EQ(Logged(R"(SET PASSWORD TO 'hunter2\)"), "SET PASSWORD TO '****");
}
