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

inline constexpr std::array storage_modes{
    memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL,
    memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
};

}  // namespace

std::string_view StorageModeToString(memgraph::storage::StorageMode storage_mode) {
  switch (storage_mode) {
    case memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL:
      return "IN_MEMORY_ANALYTICAL";
    case memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL:
      return "IN_MEMORY_TRANSACTIONAL";
  }
}

class StorageModeTest : public ::testing::TestWithParam<memgraph::storage::StorageMode> {
 public:
  struct PrintStringParamToName {
    std::string operator()(const testing::TestParamInfo<memgraph::storage::StorageMode> &info) {
      return std::string(StorageModeToString(static_cast<memgraph::storage::StorageMode>(info.param)));
    }
  };
};

// you should be able to see nodes if there is analytics mode
TEST_P(StorageModeTest, Mode) {
  const memgraph::storage::StorageMode storage_mode = GetParam();

  memgraph::storage::Storage storage{
      {.transaction{.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}}};
  storage.SetStorageMode(storage_mode);
  auto creator = storage.Access();
  auto other_analytics_mode_reader = storage.Access();

  ASSERT_EQ(VerticesCount(creator), 0);
  ASSERT_EQ(VerticesCount(other_analytics_mode_reader), 0);

  static constexpr int vertex_creation_count = 10;
  {
    for (size_t i = 1; i <= vertex_creation_count; i++) {
      creator.CreateVertex();

      int64_t expected_vertices_count = storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL ? i : 0;
      ASSERT_EQ(VerticesCount(creator), expected_vertices_count);
      ASSERT_EQ(VerticesCount(other_analytics_mode_reader), expected_vertices_count);
    }
  }

  ASSERT_FALSE(creator.Commit().HasError());
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageModeTests, StorageModeTest, ::testing::ValuesIn(storage_modes),
                        StorageModeTest::PrintStringParamToName());
