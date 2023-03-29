#include <gtest/gtest.h>
#include <string_view>

#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage_test_utils.hpp"

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

  ASSERT_EQ(CountVertices(creator, memgraph::storage::View::OLD), 0);
  ASSERT_EQ(CountVertices(other_analytics_mode_reader, memgraph::storage::View::OLD), 0);

  static constexpr int vertex_creation_count = 10;
  {
    for (size_t i = 1; i <= vertex_creation_count; i++) {
      creator.CreateVertex();

      int64_t expected_vertices_count = storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL ? i : 0;
      ASSERT_EQ(CountVertices(creator, memgraph::storage::View::OLD), expected_vertices_count);
      ASSERT_EQ(CountVertices(other_analytics_mode_reader, memgraph::storage::View::OLD), expected_vertices_count);
    }
  }

  ASSERT_FALSE(creator.Commit().HasError());
}

INSTANTIATE_TEST_CASE_P(ParameterizedStorageModeTests, StorageModeTest, ::testing::ValuesIn(storage_modes),
                        StorageModeTest::PrintStringParamToName());
