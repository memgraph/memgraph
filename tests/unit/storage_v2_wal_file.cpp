// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <string_view>

#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage_test_utils.hpp"
#include "utils/file.hpp"
#include "utils/file_locker.hpp"
#include "utils/memory.hpp"
#include "utils/uuid.hpp"

// Helper function used to convert between enum types.
memgraph::storage::durability::WalDeltaData::Type StorageMetadataOperationToWalDeltaDataType(
    memgraph::storage::durability::StorageMetadataOperation operation) {
  switch (operation) {
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_INDEX_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_INDEX_DROP;
    case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::EDGE_INDEX_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::EDGE_INDEX_DROP;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_SET:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_INDEX_STATS_SET;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_CLEAR:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_SET:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET;
    case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_CLEAR:
      return memgraph::storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR;
    case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::TEXT_INDEX_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::TEXT_INDEX_DROP;
    case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP;
    case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_CREATE:
      return memgraph::storage::durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE;
    case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_DROP:
      return memgraph::storage::durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP;
  }
}

// This class mimics the internals of the storage to generate the deltas.
class DeltaGenerator final {
 public:
  class Transaction final {
   private:
    friend class DeltaGenerator;

    explicit Transaction(DeltaGenerator *gen)
        : gen_(gen),
          transaction_(gen->transaction_id_++, gen->timestamp_++, memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
                       gen->storage_mode_, false, false) {}

   public:
    memgraph::storage::Vertex *CreateVertex() {
      auto gid = memgraph::storage::Gid::FromUint(gen_->vertices_count_++);
      auto delta = memgraph::storage::CreateDeleteObjectDelta(&transaction_);
      auto &it = gen_->vertices_.emplace_back(gid, delta);
      if (delta != nullptr) {
        delta->prev.Set(&it);
      }
      if (transaction_.storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) return &it;
      {
        memgraph::storage::durability::WalDeltaData data;
        data.type = memgraph::storage::durability::WalDeltaData::Type::VERTEX_CREATE;
        data.vertex_create_delete.gid = gid;
        data_.push_back(data);
      }
      return &it;
    }

    void DeleteVertex(memgraph::storage::Vertex *vertex) {
      memgraph::storage::CreateAndLinkDelta(&transaction_, &*vertex, memgraph::storage::Delta::RecreateObjectTag());
      if (transaction_.storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) return;
      {
        memgraph::storage::durability::WalDeltaData data;
        data.type = memgraph::storage::durability::WalDeltaData::Type::VERTEX_DELETE;
        data.vertex_create_delete.gid = vertex->gid;
        data_.push_back(data);
      }
    }

    void AddLabel(memgraph::storage::Vertex *vertex, const std::string &label) {
      auto label_id = memgraph::storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.push_back(label_id);
      memgraph::storage::CreateAndLinkDelta(&transaction_, &*vertex, memgraph::storage::Delta::RemoveLabelTag(),
                                            label_id);
      if (transaction_.storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) return;
      {
        memgraph::storage::durability::WalDeltaData data;
        data.type = memgraph::storage::durability::WalDeltaData::Type::VERTEX_ADD_LABEL;
        data.vertex_add_remove_label.gid = vertex->gid;
        data.vertex_add_remove_label.label = label;
        data_.push_back(data);
      }
    }

    void RemoveLabel(memgraph::storage::Vertex *vertex, const std::string &label) {
      auto label_id = memgraph::storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.erase(std::find(vertex->labels.begin(), vertex->labels.end(), label_id));
      memgraph::storage::CreateAndLinkDelta(&transaction_, &*vertex, memgraph::storage::Delta::AddLabelTag(), label_id);
      if (transaction_.storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) return;
      {
        memgraph::storage::durability::WalDeltaData data;
        data.type = memgraph::storage::durability::WalDeltaData::Type::VERTEX_REMOVE_LABEL;
        data.vertex_add_remove_label.gid = vertex->gid;
        data.vertex_add_remove_label.label = label;
        data_.push_back(data);
      }
    }

    void SetProperty(memgraph::storage::Vertex *vertex, const std::string &property,
                     const memgraph::storage::PropertyValue &value) {
      auto property_id = memgraph::storage::PropertyId::FromUint(gen_->mapper_.NameToId(property));
      auto &props = vertex->properties;
      auto old_value = props.GetProperty(property_id);
      memgraph::storage::CreateAndLinkDelta(&transaction_, &*vertex, memgraph::storage::Delta::SetPropertyTag(),
                                            property_id, old_value);
      props.SetProperty(property_id, value);
      if (transaction_.storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) return;
      {
        memgraph::storage::durability::WalDeltaData data;
        data.type = memgraph::storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY;
        data.vertex_edge_set_property.gid = vertex->gid;
        data.vertex_edge_set_property.property = property;
        // We don't store the property value here. That is because the storage
        // generates multiple `SetProperty` deltas using only the final values
        // of the property. The intermediate values aren't encoded. The value is
        // later determined in the `Finalize` function.
        data_.push_back(data);
      }
    }

    void Finalize(bool append_transaction_end = true) {
      auto commit_timestamp = gen_->timestamp_++;
      if (transaction_.deltas.empty()) return;
      for (const auto &delta : transaction_.deltas) {
        auto owner = delta.prev.Get();
        while (owner.type == memgraph::storage::PreviousPtr::Type::DELTA) {
          owner = owner.delta->prev.Get();
        }
        if (owner.type == memgraph::storage::PreviousPtr::Type::VERTEX) {
          gen_->wal_file_.AppendDelta(delta, *owner.vertex, commit_timestamp);
        } else if (owner.type == memgraph::storage::PreviousPtr::Type::EDGE) {
          gen_->wal_file_.AppendDelta(delta, *owner.edge, commit_timestamp);
        } else {
          LOG_FATAL("Invalid delta owner!");
        }
      }
      if (append_transaction_end) {
        gen_->wal_file_.AppendTransactionEnd(commit_timestamp);
        if (gen_->valid_) {
          gen_->UpdateStats(commit_timestamp, transaction_.deltas.size() + 1);
          for (auto &data : data_) {
            if (data.type == memgraph::storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY) {
              // We need to put the final property value into the SET_PROPERTY
              // delta.
              auto vertex =
                  std::find(gen_->vertices_.begin(), gen_->vertices_.end(), data.vertex_edge_set_property.gid);
              ASSERT_NE(vertex, gen_->vertices_.end());
              auto property_id = memgraph::storage::PropertyId::FromUint(
                  gen_->mapper_.NameToId(data.vertex_edge_set_property.property));
              data.vertex_edge_set_property.value = vertex->properties.GetProperty(property_id);
            }
            gen_->data_.emplace_back(commit_timestamp, data);
          }
          memgraph::storage::durability::WalDeltaData data{
              .type = memgraph::storage::durability::WalDeltaData::Type::TRANSACTION_END};
          gen_->data_.emplace_back(commit_timestamp, data);
        }
      } else {
        gen_->valid_ = false;
      }
    }

    void FinalizeOperationTx() {
      auto timestamp = gen_->timestamp_;
      gen_->wal_file_.AppendTransactionEnd(timestamp);
      if (gen_->valid_) {
        gen_->UpdateStats(timestamp, 1);
        memgraph::storage::durability::WalDeltaData data{
            .type = memgraph::storage::durability::WalDeltaData::Type::TRANSACTION_END};
        gen_->data_.emplace_back(timestamp, data);
      }
    }

   private:
    DeltaGenerator *gen_;
    memgraph::storage::Transaction transaction_;
    std::vector<memgraph::storage::durability::WalDeltaData> data_;
  };

  using DataT = std::vector<std::pair<uint64_t, memgraph::storage::durability::WalDeltaData>>;

  DeltaGenerator(const std::filesystem::path &data_directory, bool properties_on_edges, uint64_t seq_num,
                 memgraph::storage::StorageMode storage_mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL)
      : uuid_(memgraph::utils::GenerateUUID()),
        epoch_id_(memgraph::utils::GenerateUUID()),
        seq_num_(seq_num),
        wal_file_(data_directory, uuid_, epoch_id_, {.properties_on_edges = properties_on_edges}, &mapper_, seq_num,
                  &file_retainer_),
        storage_mode_(storage_mode) {}

  Transaction CreateTransaction() { return Transaction(this); }

  void ResetTransactionIds() {
    transaction_id_ = memgraph::storage::kTransactionInitialId;
    timestamp_ = memgraph::storage::kTimestampInitialId;
    valid_ = false;
  }

  void AppendOperation(memgraph::storage::durability::StorageMetadataOperation operation, const std::string &label,
                       const std::set<std::string, std::less<>> properties = {}, const std::string &stats = {}) {
    auto label_id = memgraph::storage::LabelId::FromUint(mapper_.NameToId(label));
    std::set<memgraph::storage::PropertyId> property_ids;
    for (const auto &property : properties) {
      property_ids.insert(memgraph::storage::PropertyId::FromUint(mapper_.NameToId(property)));
    }
    memgraph::storage::LabelIndexStats l_stats{};
    memgraph::storage::LabelPropertyIndexStats lp_stats{};
    if (!stats.empty()) {
      if (operation == memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_SET) {
        ASSERT_TRUE(FromJson(stats, l_stats));
      } else if (operation == memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_SET) {
        ASSERT_TRUE(FromJson(stats, lp_stats));
      } else {
        ASSERT_TRUE(false) << "Unexpected statistics operation!";
      }
    }
    wal_file_.AppendOperation(operation, std::nullopt, label_id, property_ids, l_stats, lp_stats, timestamp_);
    if (valid_) {
      UpdateStats(timestamp_, 1);
      memgraph::storage::durability::WalDeltaData data;
      data.type = StorageMetadataOperationToWalDeltaDataType(operation);
      switch (operation) {
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_CLEAR: /* Special case
                                                                                                         */
          data.operation_label.label = label;
          break;
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_SET:
          data.operation_label_stats.label = label;
          data.operation_label_stats.stats = stats;
          break;
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_DROP:
          data.operation_label_property.label = label;
          data.operation_label_property.property = *properties.begin();
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_SET:
          data.operation_label_property_stats.label = label;
          data.operation_label_property_stats.property = *properties.begin();
          data.operation_label_property_stats.stats = stats;
          break;
        case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_DROP:
          data.operation_label_properties.label = label;
          data.operation_label_properties.properties = properties;
          break;
        case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_DROP:
          MG_ASSERT(false, "Invalid function call!");
      }
      data_.emplace_back(timestamp_, data);
    }
  }

  void AppendEdgeTypeOperation(memgraph::storage::durability::StorageMetadataOperation operation,
                               const std::string &edge_type) {
    auto edge_type_id = memgraph::storage::EdgeTypeId::FromUint(mapper_.NameToId(edge_type));
    wal_file_.AppendOperation(operation, edge_type_id, timestamp_);
    if (valid_) {
      UpdateStats(timestamp_, 1);
      memgraph::storage::durability::WalDeltaData data;
      data.type = StorageMetadataOperationToWalDeltaDataType(operation);
      switch (operation) {
        case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::EDGE_TYPE_INDEX_DROP:
          data.operation_edge_type.edge_type = edge_type;
          break;
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_CLEAR:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_INDEX_STATS_SET:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::TEXT_INDEX_DROP:
        case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_DROP:;
        case memgraph::storage::durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_SET:
        case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_CREATE:
        case memgraph::storage::durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_DROP:
          MG_ASSERT(false, "Invalid function call!");
      }
      data_.emplace_back(timestamp_, data);
    }
  }

  uint64_t GetPosition() { return wal_file_.GetSize(); }

  memgraph::storage::durability::WalInfo GetInfo() {
    return {.offset_metadata = 0,
            .offset_deltas = 0,
            .uuid = uuid_,
            .epoch_id = epoch_id_,
            .seq_num = seq_num_,
            .from_timestamp = tx_from_,
            .to_timestamp = tx_to_,
            .num_deltas = deltas_count_};
  }

  DataT GetData() { return data_; }

 private:
  void UpdateStats(uint64_t timestamp, uint64_t count) {
    if (deltas_count_ == 0) {
      tx_from_ = timestamp;
    }
    tx_to_ = timestamp;
    deltas_count_ += count;
  }

  std::string uuid_;
  std::string epoch_id_;
  uint64_t seq_num_;

  uint64_t transaction_id_{memgraph::storage::kTransactionInitialId};
  uint64_t timestamp_{memgraph::storage::kTimestampInitialId};
  uint64_t vertices_count_{0};
  std::list<memgraph::storage::Vertex> vertices_;
  memgraph::storage::NameIdMapper mapper_;

  memgraph::storage::durability::WalFile wal_file_;

  DataT data_;

  uint64_t deltas_count_{0};
  uint64_t tx_from_{0};
  uint64_t tx_to_{0};
  uint64_t valid_{true};

  memgraph::utils::FileRetainer file_retainer_;

  memgraph::storage::StorageMode storage_mode_;
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define TRANSACTION(append_transaction_end, ops) \
  {                                              \
    auto tx = gen.CreateTransaction();           \
    ops;                                         \
    tx.Finalize(append_transaction_end);         \
  }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define OPERATION(op, ...) gen.AppendOperation(memgraph::storage::durability::StorageMetadataOperation::op, __VA_ARGS__)

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define OPERATION_TX(op, ...)          \
  {                                    \
    auto tx = gen.CreateTransaction(); \
    OPERATION(op, __VA_ARGS__);        \
    tx.FinalizeOperationTx();          \
  }

void AssertWalInfoEqual(const memgraph::storage::durability::WalInfo &a,
                        const memgraph::storage::durability::WalInfo &b) {
  ASSERT_EQ(a.uuid, b.uuid);
  ASSERT_EQ(a.epoch_id, b.epoch_id);
  ASSERT_EQ(a.seq_num, b.seq_num);
  ASSERT_EQ(a.from_timestamp, b.from_timestamp);
  ASSERT_EQ(a.to_timestamp, b.to_timestamp);
  ASSERT_EQ(a.num_deltas, b.num_deltas);
}

void AssertWalDataEqual(const DeltaGenerator::DataT &data, const std::filesystem::path &path) {
  auto info = memgraph::storage::durability::ReadWalInfo(path);
  memgraph::storage::durability::Decoder wal;
  wal.Initialize(path, memgraph::storage::durability::kWalMagic);
  wal.SetPosition(info.offset_deltas);
  DeltaGenerator::DataT current;
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    auto timestamp = memgraph::storage::durability::ReadWalDeltaHeader(&wal);
    current.emplace_back(timestamp, memgraph::storage::durability::ReadWalDeltaData(&wal));
  }
  ASSERT_EQ(data.size(), current.size());
  ASSERT_EQ(data, current);
}

class WalFileTest : public ::testing::TestWithParam<bool> {
 public:
  WalFileTest() = default;

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::vector<std::filesystem::path> GetFilesList() {
    std::vector<std::filesystem::path> ret;
    for (auto &item : std::filesystem::directory_iterator(storage_directory)) {
      ret.push_back(item.path());
    }
    std::sort(ret.begin(), ret.end());
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_storage_v2_wal_file"};

 private:
  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }
};

INSTANTIATE_TEST_SUITE_P(EdgesWithProperties, WalFileTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(EdgesWithoutProperties, WalFileTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, EmptyFile) {
  { DeltaGenerator gen(storage_directory, GetParam(), 5); }
  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 0);
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_SIMPLE_TEST(name, ops)                                                        \
  TEST_P(WalFileTest, name) {                                                                  \
    memgraph::storage::durability::WalInfo info;                                               \
    DeltaGenerator::DataT data;                                                                \
                                                                                               \
    {                                                                                          \
      DeltaGenerator gen(storage_directory, GetParam(), 5);                                    \
      ops;                                                                                     \
      info = gen.GetInfo();                                                                    \
      data = gen.GetData();                                                                    \
    }                                                                                          \
                                                                                               \
    auto wal_files = GetFilesList();                                                           \
    ASSERT_EQ(wal_files.size(), 1);                                                            \
                                                                                               \
    if (info.num_deltas == 0) {                                                                \
      ASSERT_THROW(memgraph::storage::durability::ReadWalInfo(wal_files.front()),              \
                   memgraph::storage::durability::RecoveryFailure);                            \
    } else {                                                                                   \
      AssertWalInfoEqual(info, memgraph::storage::durability::ReadWalInfo(wal_files.front())); \
      AssertWalDataEqual(data, wal_files.front());                                             \
    }                                                                                          \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithEnd, { TRANSACTION(true, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithoutEnd, { TRANSACTION(false, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(OperationSingle, { OPERATION_TX(LABEL_INDEX_CREATE, "hello"); });

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsEnd00, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsEnd01, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsEnd10, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsEnd11, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_00, {
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_01, {
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_10, {
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_11, {
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation0_0, {
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation0_1, {
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation1_0, {
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation1_1, {
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation00_, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation01_, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation10_, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation11_, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(AllTransactionOperationsWithEnd, {
  TRANSACTION(true, {
    auto vertex1 = tx.CreateVertex();
    auto vertex2 = tx.CreateVertex();
    tx.AddLabel(vertex1, "test");
    tx.AddLabel(vertex2, "hello");
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue("nandare"));
    tx.RemoveLabel(vertex1, "test");
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue(123));
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue());
    tx.DeleteVertex(vertex1);
  });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(AllTransactionOperationsWithoutEnd, {
  TRANSACTION(false, {
    auto vertex1 = tx.CreateVertex();
    auto vertex2 = tx.CreateVertex();
    tx.AddLabel(vertex1, "test");
    tx.AddLabel(vertex2, "hello");
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue("nandare"));
    tx.RemoveLabel(vertex1, "test");
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue(123));
    tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue());
    tx.DeleteVertex(vertex1);
  });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(MultiOpTransaction, {
  namespace ms = memgraph::storage;
  auto l_stats = ms::ToJson(ms::LabelIndexStats{12, 34});
  auto lp_stats = ms::ToJson(ms::LabelPropertyIndexStats{98, 76, 54., 32., 10.});
  auto tx = gen.CreateTransaction();
  OPERATION(LABEL_PROPERTY_INDEX_STATS_SET, "hello", {"world"}, lp_stats);
  OPERATION(LABEL_PROPERTY_INDEX_STATS_SET, "hello", {"and"}, lp_stats);
  OPERATION(LABEL_PROPERTY_INDEX_STATS_SET, "hello", {"universe"}, lp_stats);
  OPERATION(LABEL_INDEX_STATS_SET, "hello", {}, l_stats);
  tx.FinalizeOperationTx();
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(AllGlobalOperations, {
  namespace ms = memgraph::storage;
  OPERATION_TX(LABEL_INDEX_CREATE, "hello");
  OPERATION_TX(LABEL_INDEX_DROP, "hello");
  auto l_stats = ms::ToJson(ms::LabelIndexStats{12, 34});
  OPERATION_TX(LABEL_INDEX_STATS_SET, "hello", {}, l_stats);
  OPERATION_TX(LABEL_INDEX_STATS_CLEAR, "hello");
  OPERATION_TX(LABEL_PROPERTY_INDEX_CREATE, "hello", {"world"});
  OPERATION_TX(LABEL_PROPERTY_INDEX_DROP, "hello", {"world"});
  auto lp_stats = ms::ToJson(ms::LabelPropertyIndexStats{98, 76, 54., 32., 10.});
  OPERATION_TX(LABEL_PROPERTY_INDEX_STATS_SET, "hello", {"world"}, lp_stats);
  OPERATION_TX(LABEL_PROPERTY_INDEX_STATS_CLEAR, "hello");
  OPERATION_TX(EXISTENCE_CONSTRAINT_CREATE, "hello", {"world"});
  OPERATION_TX(EXISTENCE_CONSTRAINT_DROP, "hello", {"world"});
  OPERATION_TX(UNIQUE_CONSTRAINT_CREATE, "hello", {"world", "and", "universe"});
  OPERATION_TX(UNIQUE_CONSTRAINT_DROP, "hello", {"world", "and", "universe"});
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(InvalidTransactionOrdering, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  gen.ResetTransactionIds();
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, InvalidMarker) {
  memgraph::storage::durability::WalInfo info;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    info = gen.GetInfo();
  }

  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 1);
  const auto &wal_file = wal_files.front();

  auto final_info = memgraph::storage::durability::ReadWalInfo(wal_file);
  AssertWalInfoEqual(info, final_info);

  size_t i = 0;
  for (auto marker : memgraph::storage::durability::kMarkersAll) {
    if (marker == memgraph::storage::durability::Marker::SECTION_DELTA) continue;
    auto current_file = storage_directory / fmt::format("temporary_{}", i);
    ASSERT_TRUE(std::filesystem::copy_file(wal_file, current_file));
    memgraph::utils::OutputFile file;
    file.Open(current_file, memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
    file.SetPosition(memgraph::utils::OutputFile::Position::SET, final_info.offset_deltas);
    auto value = static_cast<uint8_t>(marker);
    file.Write(&value, sizeof(value));
    file.Sync();
    file.Close();
    ASSERT_THROW(memgraph::storage::durability::ReadWalInfo(current_file),
                 memgraph::storage::durability::RecoveryFailure);
    ++i;
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, PartialData) {
  std::vector<std::pair<uint64_t, memgraph::storage::durability::WalInfo>> infos;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    TRANSACTION(true, {
      auto vertex = tx.CreateVertex();
      tx.AddLabel(vertex, "hello");
    });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    OPERATION_TX(LABEL_PROPERTY_INDEX_CREATE, "hello", {"world"});
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    TRANSACTION(true, {
      auto vertex1 = tx.CreateVertex();
      auto vertex2 = tx.CreateVertex();
      tx.AddLabel(vertex1, "test");
      tx.AddLabel(vertex2, "hello");
      tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue("nandare"));
      tx.RemoveLabel(vertex1, "test");
      tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue(123));
      tx.SetProperty(vertex2, "hello", memgraph::storage::PropertyValue());
      tx.DeleteVertex(vertex1);
    });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
  }

  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 1);
  const auto &wal_file = wal_files.front();

  AssertWalInfoEqual(infos.back().second, memgraph::storage::durability::ReadWalInfo(wal_file));

  auto current_file = storage_directory / "temporary";
  memgraph::utils::InputFile infile;
  infile.Open(wal_file);

  uint64_t pos = 0;
  for (size_t i = 0; i < infile.GetSize(); ++i) {
    if (i < infos.front().first) {
      ASSERT_THROW(memgraph::storage::durability::ReadWalInfo(current_file),
                   memgraph::storage::durability::RecoveryFailure);
    } else {
      if (i >= infos[pos + 1].first) ++pos;
      AssertWalInfoEqual(infos[pos].second, memgraph::storage::durability::ReadWalInfo(current_file));
    }
    {
      memgraph::utils::OutputFile outfile;
      outfile.Open(current_file, memgraph::utils::OutputFile::Mode::APPEND_TO_EXISTING);
      uint8_t value;
      ASSERT_TRUE(infile.Read(&value, sizeof(value)));
      outfile.Write(&value, sizeof(value));
      outfile.Sync();
      outfile.Close();
    }
  }
  ASSERT_EQ(pos, infos.size() - 2);
  AssertWalInfoEqual(infos[infos.size() - 1].second, memgraph::storage::durability::ReadWalInfo(current_file));
}

class StorageModeWalFileTest : public ::testing::TestWithParam<memgraph::storage::StorageMode> {
 public:
  StorageModeWalFileTest() = default;

  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::vector<std::filesystem::path> GetFilesList() {
    std::vector<std::filesystem::path> ret;
    for (auto &item : std::filesystem::directory_iterator(storage_directory)) {
      ret.push_back(item.path());
    }
    std::sort(ret.begin(), ret.end());
    std::reverse(ret.begin(), ret.end());
    return ret;
  }

  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_storage_v2_wal_file"};
  struct PrintStringParamToName {
    std::string operator()(const testing::TestParamInfo<memgraph::storage::StorageMode> &info) {
      return std::string(StorageModeToString(static_cast<memgraph::storage::StorageMode>(info.param)));
    }
  };

 private:
  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(StorageModeWalFileTest, StorageModeData) {
  std::vector<std::pair<uint64_t, memgraph::storage::durability::WalInfo>> infos;
  const memgraph::storage::StorageMode storage_mode = GetParam();

  {
    DeltaGenerator gen(storage_directory, true, 5, storage_mode);
    auto tx = gen.CreateTransaction();
    tx.CreateVertex();
    tx.Finalize(true);
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());

    size_t num_expected_deltas = storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL ? 0 : 2;
    ASSERT_EQ(infos[0].second.num_deltas, num_expected_deltas);

    auto wal_files = GetFilesList();
    size_t num_expected_wal_files = 1;
    ASSERT_EQ(num_expected_wal_files, wal_files.size());

    if (storage_mode == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) {
      DeltaGenerator gen_empty(storage_directory, true, 5, storage_mode);
      ASSERT_EQ(gen.GetPosition(), gen_empty.GetPosition());
    }
  }
}

INSTANTIATE_TEST_SUITE_P(ParameterizedWalStorageModeTests, StorageModeWalFileTest, ::testing::ValuesIn(storage_modes),
                         StorageModeWalFileTest::PrintStringParamToName());
