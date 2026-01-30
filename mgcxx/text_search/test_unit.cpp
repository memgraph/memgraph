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

#include <thread>
#include "gtest/gtest.h"

#include "test_util.hpp"

TEST(text_search_test_case, simple_test1) {
  try {
    auto index_name = "tantivy_index_simple_test1";
    auto index_config = mgcxx::text_search::IndexConfig{.mappings = dummy_mappings1().dump()};
    auto context = mgcxx::text_search::create_index(index_name, index_config);

    for (const auto &doc : dummy_data1(5, 5)) {
      measure_time_diff<int>("add", [&]() {
        mgcxx::text_search::add_document(context, doc, false);
        return 0;
      });
    }

    // wait for delay to ensure all documents are indexed
    while (mgcxx::text_search::get_num_docs(context) < 5) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    mgcxx::text_search::SearchInput search_input = {
        .search_fields = {"metadata"}, .search_query = "data.key1:AWESOME", .return_fields = {"data"}};
    auto result1 = measure_time_diff<mgcxx::text_search::SearchOutput>(
        "search1", [&]() { return mgcxx::text_search::search(context, search_input); });
    ASSERT_EQ(result1.docs.size(), 5);
    for (uint64_t i = 0; i < 10; ++i) {
      auto result = measure_time_diff<mgcxx::text_search::SearchOutput>(
          fmt::format("search{}", i), [&]() { return mgcxx::text_search::search(context, search_input); });
    }

    nlohmann::json aggregation_query = {};
    aggregation_query["count"]["value_count"]["field"] = "metadata.txid";
    mgcxx::text_search::SearchInput aggregate_input = {
        .search_fields = {"data"},
        .search_query = "data.key1:AWESOME",
        .aggregation_query = aggregation_query.dump(),
    };
    auto aggregation_result = nlohmann::json::parse(mgcxx::text_search::aggregate(context, aggregate_input).data);
    EXPECT_NEAR(aggregation_result["count"]["value"], 5, 1e-6);
    mgcxx::text_search::drop_index(std::move(context));
  } catch (const ::rust::Error &error) {
    FAIL() << "Test failed: " << error.what();
  }
}

TEST(text_search_test_case, simple_test2) {
  try {
    auto index_name = "tantivy_index_simple_test2";
    auto index_config = mgcxx::text_search::IndexConfig{.mappings = dummy_mappings2().dump()};
    auto context = mgcxx::text_search::create_index(index_name, index_config);

    for (const auto &doc : dummy_data2(2, 1)) {
      measure_time_diff<int>("add", [&]() {
        mgcxx::text_search::add_document(context, doc, false);
        return 0;
      });
    }
    // wait for delay to ensure all documents are indexed
    while (mgcxx::text_search::get_num_docs(context) < 2) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    mgcxx::text_search::SearchInput search_input = {
        .search_fields = {"gid"}, .search_query = fmt::format("{}", 0), .return_fields = {"data"}};
    auto result = mgcxx::text_search::search(context, search_input);
    ASSERT_EQ(result.docs.size(), 1);
    mgcxx::text_search::drop_index(std::move(context));
  } catch (const ::rust::Error &error) {
    FAIL() << "Test failed: " << error.what();
  }
}

TEST(text_search_test_case, mappings) {
  try {
    constexpr auto index_name = "tantivy_index_mappings";
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["prop1"] = {{"type", "u64"}, {"fast", true}, {"indexed", true}};
    mappings["properties"]["prop2"] = {{"type", "text"}, {"stored", true}, {"text", true}, {"fast", true}};
    mappings["properties"]["prop3"] = {{"type", "json"}, {"stored", true}, {"text", true}, {"fast", true}};
    mappings["properties"]["prop4"] = {{"type", "bool"}, {"stored", true}, {"text", true}, {"fast", true}};
    auto context =
        mgcxx::text_search::create_index(index_name, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()});

    // NOTE: This test just verifies the code can be called, add deeper test
    // when improving extract_schema.
    // TODO(gitbuda): Implement full range of extract_schema options.
    mgcxx::text_search::SearchInput search_input = {
        .search_fields = {"prop1"}, .search_query = "bla", .return_fields = {"data"}};
    mgcxx::text_search::search(context, search_input);
    mgcxx::text_search::drop_index(std::move(context));
  } catch (const ::rust::Error &error) {
    std::cout << error.what() << std::endl;
    EXPECT_STREQ(error.what(),
                 "The field does not exist: 'data' inside "
                 "\"tantivy_index_mappings\" text search index");
  }
}

TEST(text_search_test_case, limit_test) {
  try {
    auto index_name =
        fmt::format("tantivy_index_limit_test_{}", std::chrono::duration_cast<std::chrono::microseconds>(
                                                       std::chrono::high_resolution_clock::now().time_since_epoch())
                                                       .count());
    auto index_config = mgcxx::text_search::IndexConfig{.mappings = dummy_mappings1().dump()};
    auto context = mgcxx::text_search::create_index(index_name, index_config);

    for (const auto &doc : dummy_data1(10, 10)) {
      mgcxx::text_search::add_document(context, doc, false);
    }
    // wait for all documents to be indexed
    while (mgcxx::text_search::get_num_docs(context) < 10) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    mgcxx::text_search::SearchInput search_input_limit3 = {.search_fields = {"metadata"},
                                                           .search_query = "data.key1:AWESOME",
                                                           .return_fields = {"data"},
                                                           .aggregation_query = "",
                                                           .limit = 3};
    auto result_limit3 = mgcxx::text_search::search(context, search_input_limit3);
    ASSERT_EQ(result_limit3.docs.size(), 3);
    for (const auto &doc : result_limit3.docs) {
      ASSERT_GT(doc.score, 0.0f);  // Scores should be positive
    }

    mgcxx::text_search::SearchInput regex_search_input = {.search_fields = {"data"},
                                                          .search_query = ".*",
                                                          .return_fields = {"data"},
                                                          .aggregation_query = "",
                                                          .limit = 2};
    auto regex_result = mgcxx::text_search::regex_search(context, regex_search_input);
    ASSERT_EQ(regex_result.docs.size(), 2);

    mgcxx::text_search::drop_index(std::move(context));
  } catch (const ::rust::Error &error) {
    FAIL() << "Test failed: " << error.what();
  }
}

// TODO(gitbuda): Make a gtest main lib and link agains other test binaries.
int main(int argc, char *argv[]) {
  // init tantivy engine (actually logging setup, should be called once per
  // process, early)
  mgcxx::text_search::init("todo");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
