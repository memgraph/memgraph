#include <chrono>
#include <thread>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <storage/v2/storage.hpp>
#include <storage/v2/property_value.hpp>

TEST(ReplicationTest, BasicSynchronousReplicationTest) {
  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_storage_v2_replication"};
  
  storage::Storage main_store(
      {.durability = {
           .storage_directory = storage_directory,
           .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::
               PERIODIC_SNAPSHOT_WITH_WAL,
       }});
  main_store.SetReplicationState(storage::ReplicationState::MAIN);

  storage::Storage replica_store(
      {.durability = {
           .storage_directory = storage_directory,
           .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::
               PERIODIC_SNAPSHOT_WITH_WAL,
       }});
  replica_store.SetReplicationState(storage::ReplicationState::REPLICA);

  const size_t expected_vertex_count = 1000U;
  {
    auto acc = main_store.Access();
    for (size_t i = 0; i < expected_vertex_count; ++i) {
      auto vec = acc.CreateVertex();
      ASSERT_TRUE(vec.AddLabel(main_store.NameToLabel(fmt::format("l{}", i)))
                      .HasValue());
      ASSERT_TRUE(
          vec.SetProperty(main_store.NameToProperty("hello"),
                          storage::PropertyValue(fmt::format("world{}", i)))
              .HasValue());
    }
    ASSERT_FALSE(acc.Commit().HasError());
  }

  {
    auto acc = replica_store.Access();
    int vertex_count = 0;
    for (const auto vertex_acc : acc.Vertices(storage::View::OLD)) {
      const auto has_label = vertex_acc.HasLabel(
          replica_store.NameToLabel(fmt::format("l{}", vertex_count)),
          storage::View::OLD);
      ASSERT_TRUE(has_label.HasValue() && has_label.GetValue());
      const auto property_value = vertex_acc.GetProperty(
          replica_store.NameToProperty("hello"), storage::View::OLD);
      ASSERT_TRUE(property_value.HasValue());
      ASSERT_EQ(property_value.GetValue(),
                storage::PropertyValue(fmt::format("world{}", vertex_count)));
      ++vertex_count;
    }
    ASSERT_EQ(vertex_count, expected_vertex_count);
    ASSERT_FALSE(acc.Commit().HasError());
  }
}
