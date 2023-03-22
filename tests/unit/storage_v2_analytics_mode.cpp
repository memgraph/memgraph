

// what do I want to test:
// first I need to test that no deltas are created
// second I need to test that transaction commits normally
// third I need to test that no wals are created
// forth I need to test that when I create snapshot it works normally
// fifth I need to test that you can change mode
// sixth I need to test that you can't change mode when there is other transaction active
#include <gtest/gtest.h>
#include <string_view>

#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace {

int64_t VerticesCount(memgraph::storage::Storage::Accessor &accessor) {
  int64_t count{0};
  for ([[maybe_unused]] const auto &vertex : accessor.Vertices(memgraph::storage::View::OLD)) {
    ++count;
  }
  return count;
}

inline constexpr std::array analytics_modes{
    memgraph::storage::AnalyticsMode::ON,
    memgraph::storage::AnalyticsMode::OFF,
};

}  // namespace

std::string_view AnalyticsModeToString(memgraph::storage::AnalyticsMode analytics_mode) {
  switch (analytics_mode) {
    case memgraph::storage::AnalyticsMode::ON:
      return "ON";
    case memgraph::storage::AnalyticsMode::OFF:
      return "OFF";
  }
}

class StorageAnalyticsModeTest : public ::testing::TestWithParam<memgraph::storage::AnalyticsMode> {
 public:
  struct PrintStringParamToName {
    std::string operator()(const testing::TestParamInfo<memgraph::storage::AnalyticsMode> &info) {
      return std::string(AnalyticsModeToString(static_cast<memgraph::storage::AnalyticsMode>(info.param)));
    }
  };
};

// you should be able to see nodes if there is analytics mode
TEST_P(StorageAnalyticsModeTest, Mode) {
  const memgraph::storage::AnalyticsMode analytics_mode = GetParam();

  memgraph::storage::Storage storage{
      {.transaction{.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}}};
  storage.SetAnalyticsMode(analytics_mode);
  auto creator = storage.Access();
  auto other_analytics_mode_reader = storage.Access();

  ASSERT_EQ(VerticesCount(creator), 0);
  ASSERT_EQ(VerticesCount(other_analytics_mode_reader), 0);

  static constexpr int vertex_creation_count = 10;
  {
    for (size_t i = 1; i <= vertex_creation_count; i++) {
      creator.CreateVertex();

      int64_t expected_vertices_count = analytics_mode == memgraph::storage::AnalyticsMode::ON ? i : 0;
      ASSERT_EQ(VerticesCount(creator), expected_vertices_count);
      ASSERT_EQ(VerticesCount(other_analytics_mode_reader), expected_vertices_count);
    }
  }

  ASSERT_FALSE(creator.Commit().HasError());
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageAnalyticsModeTests, StorageAnalyticsModeTest,
                        ::testing::ValuesIn(analytics_modes), StorageAnalyticsModeTest::PrintStringParamToName());
