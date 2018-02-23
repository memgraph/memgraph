#include <experimental/optional>

#include "gtest/gtest.h"

#include "communication/rpc/server.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "storage/types.hpp"

template <typename TId>
class DistributedConcurrentIdMapperTest : public ::testing::Test {
  const std::string kLocal{"127.0.0.1"};

 protected:
  communication::rpc::Server master_server_{{kLocal, 0}};
  std::experimental::optional<storage::MasterConcurrentIdMapper<TId>>
      master_mapper_;
  std::experimental::optional<storage::WorkerConcurrentIdMapper<TId>>
      worker_mapper_;

  void SetUp() override {
    master_mapper_.emplace(master_server_);
    worker_mapper_.emplace(master_server_.endpoint());
  }
  void TearDown() override {
    worker_mapper_ = std::experimental::nullopt;
    master_mapper_ = std::experimental::nullopt;
  }
};

typedef ::testing::Types<storage::Label, storage::EdgeType, storage::Property>
    GraphDbTestTypes;
TYPED_TEST_CASE(DistributedConcurrentIdMapperTest, GraphDbTestTypes);

TYPED_TEST(DistributedConcurrentIdMapperTest, Basic) {
  auto &master = this->master_mapper_.value();
  auto &worker = this->worker_mapper_.value();

  auto id1 = master.value_to_id("v1");
  EXPECT_EQ(worker.id_to_value(id1), "v1");
  EXPECT_EQ(worker.value_to_id("v1"), id1);

  auto id2 = worker.value_to_id("v2");
  EXPECT_EQ(master.id_to_value(id2), "v2");
  EXPECT_EQ(master.value_to_id("v2"), id2);

  EXPECT_NE(id1, id2);
}
