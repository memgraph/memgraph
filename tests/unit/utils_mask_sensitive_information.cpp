// Copyright 2025 Memgraph Ltd.
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

// Define the maskPasswords function (or include the header where it's defined)

TEST(MaskPasswords, AwsKeys) {
  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation(
                "LOAD PARQUET FROM 's3://deps.memgraph.io/nodes_100.parquet' WITH CONFIG {'aws_region': "
                "'random_region', 'aws_access_key': 'test', 'aws_secret_key': 'test1'} AS row CREATE (n:N {id: row.id, "
                "name: row.name, age: row.age, city: row.city});"),
            "LOAD PARQUET FROM 's3://deps.memgraph.io/nodes_100.parquet' WITH CONFIG {'aws_region': 'random_region', "
            "'aws_access_key': '****', 'aws_secret_key': '****'} AS row CREATE (n:N {id: row.id, name: row.name, age: "
            "row.age, city: row.city});");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("set database setting 'aws.access_key' to 'test'"),
            "set database setting 'aws.access_key' to '****'");
  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("set database setting 'aws.secret_key' to 'test'"),
            "set database setting 'aws.secret_key' to '****'");
}

TEST(MaskPasswords, GeneralCases) {
  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation(
                "CALL migrate.sql_server('example_table', {user:'memgraph', password:'password', host:'localhost', "
                "database:'demo_db'} ) YIELD row RETURN row;"),
            "CALL migrate.sql_server('example_table', {user:'memgraph', password:'****', host:'localhost', "
            "database:'demo_db'} ) YIELD row RETURN row;");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation(
                "CALL migrate.sql_server('example_table', {user:'memgraph', password:\"password\", host:'localhost', "
                "database:'demo_db'} ) YIELD row RETURN row;"),
            "CALL migrate.sql_server('example_table', {user:'memgraph', password:\"****\", host:'localhost', "
            "database:'demo_db'} ) YIELD row RETURN row;");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASSWORD TO 'newpassword' REPLACE 'oldpassword'"),
            "SET PASSWORD TO '****' REPLACE '****'");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASSWORD TO \"newpassword\" REPLACE \"oldpassword\""),
            "SET PASSWORD TO \"****\" REPLACE \"****\"");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("CREATE USER `alice@memgraph.com` IDENTIFIED BY '0042';"),
            "CREATE USER `alice@memgraph.com` IDENTIFIED BY '****';");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASSWORD FOR user_name TO 'new_password';"),
            "SET PASSWORD FOR user_name TO '****';");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASSWORD FOR user_name TO \"new_password\";"),
            "SET PASSWORD FOR user_name TO \"****\";");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASWORD FOR user_name TO 'new_password';"),
            "SET PASWORD FOR user_name TO '****';");

  EXPECT_EQ(memgraph::logging::MaskSensitiveInformation("SET PASSWORD TO 'newpassword' RPLACE 'oldpassword'"),
            "SET PASSWORD TO '****' RPLACE '****'");
}

TEST(MaskPasswords, NodePropertyCases) {
  EXPECT_EQ(
      memgraph::logging::MaskSensitiveInformation(
          "CREATE (g1:G {name: 'g1', password: 'password'}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
          "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');"),
      "CREATE (g1:G {name: 'g1', password: 'password'}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
      "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');");

  EXPECT_EQ(
      memgraph::logging::MaskSensitiveInformation(
          "CREATE (g1:G {name: 'g1', password: \"password\"}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
          "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');"),
      "CREATE (g1:G {name: 'g1', password: \"password\"}), (g2:G {name: 'g2'}), (h1:H {name: 'h1'}), (h2:H {name: "
      "'h2'}), (h3:H {name: 'h3'}), (g1)-[:CONNECTS]->(g2), (h1)-[:CONNECTS]->(h2), (h2)-[:CONNECTS]->(h3');");
}
