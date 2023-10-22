// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>

#include <gtest/gtest.h>

#include "query/plan/profile.hpp"

using namespace memgraph::query::plan;

TEST(QueryProfileTest, SimpleQuery) {
  std::chrono::duration<double> total_time{0.001};
  ProfilingStats once{2, 25, 0, "Once", {}};
  ProfilingStats produce{2, 100, 0, "Produce", {once}};

  // clang-format: off
  // +---------------+---------------+---------------+---------------+
  // | OPERATOR      | ACTUAL HITS   | RELATIVE TIME | ABSOLUTE TIME |
  // +---------------+---------------+---------------+---------------+
  // | * Produce     | 2             |  75.000000 %  |   0.750000 ms |
  // | * Once        | 2             |  25.000000 %  |   0.250000 ms |
  // +---------------+---------------+---------------+---------------+
  // clang-format: on
  auto table = ProfilingStatsToTable(ProfilingStatsWithTotalTime{produce, total_time});

  EXPECT_EQ(table[0][0].ValueString(), "* Produce");
  EXPECT_EQ(table[0][1].ValueInt(), 2);
  EXPECT_EQ(table[0][2].ValueString(), " 75.000000 %");
  EXPECT_EQ(table[0][3].ValueString(), "  0.750000 ms");

  EXPECT_EQ(table[1][0].ValueString(), "* Once");
  EXPECT_EQ(table[1][1].ValueInt(), 2);
  EXPECT_EQ(table[1][2].ValueString(), " 25.000000 %");
  EXPECT_EQ(table[1][3].ValueString(), "  0.250000 ms");

  // clang-format: off
  // {
  //   "absolute_time": 0.75,
  //   "actual_hits": 2,
  //   "children": [
  //     {
  //       "absolute_time": 0.25,
  //       "actual_hits": 2,
  //       "children": [],
  //       "name": "Once",
  //       "relative_time": 0.25
  //     }
  //   ],
  //   "name": "Produce",
  //   "relative_time": 0.75
  // }
  // clang-format: on
  auto json = ProfilingStatsToJson(ProfilingStatsWithTotalTime{produce, total_time});

  /*
   * NOTE: When one of these comparisons fails and Google Test tries to report
   * the failure, it will try to print out the JSON object. For some reason this
   * ends up recursing infinitely within Google Test's printing internals and we
   * get a segfault (stack overflow most likely).
   */
  EXPECT_EQ(json["actual_hits"], 2);
  EXPECT_EQ(json["relative_time"], 0.75);
  EXPECT_EQ(json["absolute_time"], 0.75);
  EXPECT_EQ(json["name"], "Produce");

  EXPECT_EQ(json["children"][0]["actual_hits"], 2);
  EXPECT_EQ(json["children"][0]["relative_time"], 0.25);
  EXPECT_EQ(json["children"][0]["absolute_time"], 0.25);
  EXPECT_EQ(json["children"][0]["name"], "Once");
}

TEST(QueryProfileTest, ComplicatedQuery) {
  std::chrono::duration<double> total_time{0.001};
  ProfilingStats once1{2, 5, 0, "Once", {}};
  ProfilingStats once2{1, 5, 0, "Once", {}};
  ProfilingStats once3{2, 5, 0, "Once", {}};
  ProfilingStats create_node{1, 55, 0, "CreateNode", {once2}};
  ProfilingStats scan_all{1, 20, 0, "ScanAll", {once3}};
  ProfilingStats merge{2, 85, 0, "Merge", {once1, scan_all, create_node}};
  ProfilingStats accumulate{2, 90, 0, "Accumulate", {merge}};
  ProfilingStats produce{2, 100, 0, "Produce", {accumulate}};

  // clang-format: off
  // +----------------+----------------+----------------+----------------+
  // | OPERATOR       | ACTUAL HITS    | RELATIVE TIME  | ABSOLUTE TIME  |
  // +----------------+----------------+----------------+----------------+
  // | * Produce      | 2              |  10.000000 %   |   0.100000 ms  |
  // | * Accumulate   | 2              |   5.000000 %   |   0.050000 ms  |
  // | * Merge        | 2              |   5.000000 %   |   0.050000 ms  |
  // | |\             |                |                |                |
  // | | * ScanAll    | 1              |  15.000000 %   |   0.150000 ms  |
  // | | * Once (3)   | 2              |   5.000000 %   |   0.050000 ms  |
  // | |\             |                |                |                |
  // | | * CreateNode | 1              |  50.000000 %   |   0.500000 ms  |
  // | | * Once (2)   | 1              |   5.000000 %   |   0.050000 ms  |
  // | * Once (1)     | 2              |   5.000000 %   |   0.050000 ms  |
  // +----------------+----------------+----------------+----------------+
  // clang-format: on
  auto table = ProfilingStatsToTable({produce, total_time});

  EXPECT_EQ(table[0][0].ValueString(), "* Produce");
  EXPECT_EQ(table[0][1].ValueInt(), 2);
  EXPECT_EQ(table[0][2].ValueString(), " 10.000000 %");
  EXPECT_EQ(table[0][3].ValueString(), "  0.100000 ms");

  EXPECT_EQ(table[1][0].ValueString(), "* Accumulate");
  EXPECT_EQ(table[1][1].ValueInt(), 2);
  EXPECT_EQ(table[1][2].ValueString(), "  5.000000 %");
  EXPECT_EQ(table[1][3].ValueString(), "  0.050000 ms");

  EXPECT_EQ(table[2][0].ValueString(), "* Merge");
  EXPECT_EQ(table[2][1].ValueInt(), 2);
  EXPECT_EQ(table[2][2].ValueString(), "  5.000000 %");
  EXPECT_EQ(table[2][3].ValueString(), "  0.050000 ms");

  EXPECT_EQ(table[3][0].ValueString(), "|\\");
  EXPECT_EQ(table[3][1].ValueString(), "");
  EXPECT_EQ(table[3][2].ValueString(), "");
  EXPECT_EQ(table[3][3].ValueString(), "");

  EXPECT_EQ(table[4][0].ValueString(), "| * ScanAll");
  EXPECT_EQ(table[4][1].ValueInt(), 1);
  EXPECT_EQ(table[4][2].ValueString(), " 15.000000 %");
  EXPECT_EQ(table[4][3].ValueString(), "  0.150000 ms");

  EXPECT_EQ(table[5][0].ValueString(), "| * Once");
  EXPECT_EQ(table[5][1].ValueInt(), 2);
  EXPECT_EQ(table[5][2].ValueString(), "  5.000000 %");
  EXPECT_EQ(table[5][3].ValueString(), "  0.050000 ms");

  EXPECT_EQ(table[6][0].ValueString(), "|\\");
  EXPECT_EQ(table[6][1].ValueString(), "");
  EXPECT_EQ(table[6][2].ValueString(), "");
  EXPECT_EQ(table[6][3].ValueString(), "");

  EXPECT_EQ(table[7][0].ValueString(), "| * CreateNode");
  EXPECT_EQ(table[7][1].ValueInt(), 1);
  EXPECT_EQ(table[7][2].ValueString(), " 50.000000 %");
  EXPECT_EQ(table[7][3].ValueString(), "  0.500000 ms");

  EXPECT_EQ(table[8][0].ValueString(), "| * Once");
  EXPECT_EQ(table[8][1].ValueInt(), 1);
  EXPECT_EQ(table[8][2].ValueString(), "  5.000000 %");
  EXPECT_EQ(table[8][3].ValueString(), "  0.050000 ms");

  EXPECT_EQ(table[9][0].ValueString(), "* Once");
  EXPECT_EQ(table[9][1].ValueInt(), 2);
  EXPECT_EQ(table[9][2].ValueString(), "  5.000000 %");
  EXPECT_EQ(table[9][3].ValueString(), "  0.050000 ms");

  // clang-format: off
  // {
  //   "absolute_time": 0.1,
  //   "actual_hits": 2,
  //   "children": [
  //     {
  //       "absolute_time": 0.05,
  //       "actual_hits": 2,
  //       "children": [
  //         {
  //           "absolute_time": 0.05,
  //           "actual_hits": 2,
  //           "children": [
  //             {
  //               "absolute_time": 0.05,
  //               "actual_hits": 2,
  //               "children": [],
  //               "name": "Once",
  //               "relative_time": 0.05
  //             },
  //             {
  //               "absolute_time": 0.15
  //               "actual_hits": 1,
  //               "children": [
  //                 {
  //                   "absolute_time": 0.05,
  //                   "actual_hits": 2,
  //                   "children": [],
  //                   "name": "Once",
  //                   "relative_time": 0.05
  //                 }
  //               ],
  //               "name": "ScanAll",
  //               "relative_time": 0.15
  //             },
  //             {
  //               "absolute_time": 0.50,
  //               "actual_hits": 1,
  //               "children": [
  //                 {
  //                   "absolute_time": 0.05,
  //                   "actual_hits": 1,
  //                   "children": [],
  //                   "name": "Once",
  //                   "relative_time": 0.05
  //                 }
  //               ],
  //               "name": "CreateNode",
  //               "relative_time": 0.50
  //             }
  //           ],
  //           "name": "Merge",
  //           "relative_time": 0.05
  //         }
  //       ],
  //       "name": "Accumulate",
  //       "relative_time": 0.05
  //     }
  //   ],
  //   "name": "Produce",
  //   "relative_time": 0.1,
  // }
  // clang-format: on
  auto json = ProfilingStatsToJson(ProfilingStatsWithTotalTime{produce, total_time});

  EXPECT_EQ(json["actual_hits"], 2);
  EXPECT_EQ(json["relative_time"], 0.1);
  EXPECT_EQ(json["absolute_time"], 0.1);
  EXPECT_EQ(json["name"], "Produce");

  auto children1 = json["children"];
  EXPECT_EQ(children1[0]["actual_hits"], 2);
  EXPECT_EQ(children1[0]["relative_time"], 0.05);
  EXPECT_EQ(children1[0]["absolute_time"], 0.05);
  EXPECT_EQ(children1[0]["name"], "Accumulate");

  auto children2 = children1[0]["children"];
  EXPECT_EQ(children2[0]["actual_hits"], 2);
  EXPECT_EQ(children2[0]["relative_time"], 0.05);
  EXPECT_EQ(children2[0]["absolute_time"], 0.05);
  EXPECT_EQ(children2[0]["name"], "Merge");

  auto children3 = children2[0]["children"];
  EXPECT_EQ(children3[0]["actual_hits"], 2);
  EXPECT_EQ(children3[0]["relative_time"], 0.05);
  EXPECT_EQ(children3[0]["absolute_time"], 0.05);
  EXPECT_EQ(children3[0]["name"], "Once");
  EXPECT_TRUE(children3[0]["children"].empty());

  EXPECT_EQ(children3[1]["actual_hits"], 1);
  EXPECT_EQ(children3[1]["relative_time"], 0.15);
  EXPECT_EQ(children3[1]["absolute_time"], 0.15);
  EXPECT_EQ(children3[1]["name"], "ScanAll");

  EXPECT_EQ(children3[2]["actual_hits"], 1);
  EXPECT_EQ(children3[2]["relative_time"], 0.50);
  EXPECT_EQ(children3[2]["absolute_time"], 0.50);
  EXPECT_EQ(children3[2]["name"], "CreateNode");

  auto children4 = children3[1]["children"];
  EXPECT_EQ(children4[0]["actual_hits"], 2);
  EXPECT_EQ(children4[0]["relative_time"], 0.05);
  EXPECT_EQ(children4[0]["absolute_time"], 0.05);
  EXPECT_EQ(children4[0]["name"], "Once");
  EXPECT_TRUE(children4[0]["children"].empty());

  auto children5 = children3[2]["children"];
  EXPECT_EQ(children5[0]["actual_hits"], 1);
  EXPECT_EQ(children5[0]["relative_time"], 0.05);
  EXPECT_EQ(children5[0]["absolute_time"], 0.05);
  EXPECT_EQ(children5[0]["name"], "Once");
  EXPECT_TRUE(children5[0]["children"].empty());
}
