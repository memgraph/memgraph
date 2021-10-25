// Copyright 2021 Memgraph Ltd.
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
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/file.hpp"
#include "utils/file_locker.hpp"
#include "utils/uuid.hpp"

// Helper function used to convert between enum types.
storage::durability::WalDeltaData::Type StorageGlobalOperationToWalDeltaDataType(
    storage::durability::StorageGlobalOperation operation) {
  switch (operation) {
    case storage::durability::StorageGlobalOperation::LABEL_INDEX_CREATE:
      return storage::durability::WalDeltaData::Type::LABEL_INDEX_CREATE;
    case storage::durability::StorageGlobalOperation::LABEL_INDEX_DROP:
      return storage::durability::WalDeltaData::Type::LABEL_INDEX_DROP;
    case storage::durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE:
      return storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE;
    case storage::durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP:
      return storage::durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP;
    case storage::durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE:
      return storage::durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE;
    case storage::durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP:
      return storage::durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP;
    case storage::durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE:
      return storage::durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE;
    case storage::durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP:
      return storage::durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP;
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
          transaction_(gen->transaction_id_++, gen->timestamp_++, storage::IsolationLevel::SNAPSHOT_ISOLATION) {}

   public:
    storage::Vertex *CreateVertex() {
      auto gid = storage::Gid::FromUint(gen_->vertices_count_++);
      auto delta = storage::CreateDeleteObjectDelta(&transaction_);
      auto &it = gen_->vertices_.emplace_back(gid, delta);
      delta->prev.Set(&it);
      {
        storage::durability::WalDeltaData data;
        data.type = storage::durability::WalDeltaData::Type::VERTEX_CREATE;
        data.vertex_create_delete.gid = gid;
        data_.push_back(data);
      }
      return &it;
    }

    void DeleteVertex(storage::Vertex *vertex) {
      storage::CreateAndLinkDelta(&transaction_, &*vertex, storage::Delta::RecreateObjectTag());
      {
        storage::durability::WalDeltaData data;
        data.type = storage::durability::WalDeltaData::Type::VERTEX_DELETE;
        data.vertex_create_delete.gid = vertex->gid;
        data_.push_back(data);
      }
    }

    void AddLabel(storage::Vertex *vertex, const std::string &label) {
      auto label_id = storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.push_back(label_id);
      storage::CreateAndLinkDelta(&transaction_, &*vertex, storage::Delta::RemoveLabelTag(), label_id);
      {
        storage::durability::WalDeltaData data;
        data.type = storage::durability::WalDeltaData::Type::VERTEX_ADD_LABEL;
        data.vertex_add_remove_label.gid = vertex->gid;
        data.vertex_add_remove_label.label = label;
        data_.push_back(data);
      }
    }

    void RemoveLabel(storage::Vertex *vertex, const std::string &label) {
      auto label_id = storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.erase(std::find(vertex->labels.begin(), vertex->labels.end(), label_id));
      storage::CreateAndLinkDelta(&transaction_, &*vertex, storage::Delta::AddLabelTag(), label_id);
      {
        storage::durability::WalDeltaData data;
        data.type = storage::durability::WalDeltaData::Type::VERTEX_REMOVE_LABEL;
        data.vertex_add_remove_label.gid = vertex->gid;
        data.vertex_add_remove_label.label = label;
        data_.push_back(data);
      }
    }

    void SetProperty(storage::Vertex *vertex, const std::string &property, const storage::PropertyValue &value) {
      auto property_id = storage::PropertyId::FromUint(gen_->mapper_.NameToId(property));
      auto &props = vertex->properties;
      auto old_value = props.GetProperty(property_id);
      storage::CreateAndLinkDelta(&transaction_, &*vertex, storage::Delta::SetPropertyTag(), property_id, old_value);
      props.SetProperty(property_id, value);
      {
        storage::durability::WalDeltaData data;
        data.type = storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY;
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
      for (const auto &delta : transaction_.deltas) {
        auto owner = delta.prev.Get();
        while (owner.type == storage::PreviousPtr::Type::DELTA) {
          owner = owner.delta->prev.Get();
        }
        if (owner.type == storage::PreviousPtr::Type::VERTEX) {
          gen_->wal_file_.AppendDelta(delta, *owner.vertex, commit_timestamp);
        } else if (owner.type == storage::PreviousPtr::Type::EDGE) {
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
            if (data.type == storage::durability::WalDeltaData::Type::VERTEX_SET_PROPERTY) {
              // We need to put the final property value into the SET_PROPERTY
              // delta.
              auto vertex =
                  std::find(gen_->vertices_.begin(), gen_->vertices_.end(), data.vertex_edge_set_property.gid);
              ASSERT_NE(vertex, gen_->vertices_.end());
              auto property_id =
                  storage::PropertyId::FromUint(gen_->mapper_.NameToId(data.vertex_edge_set_property.property));
              data.vertex_edge_set_property.value = vertex->properties.GetProperty(property_id);
            }
            gen_->data_.emplace_back(commit_timestamp, data);
          }
          storage::durability::WalDeltaData data{.type = storage::durability::WalDeltaData::Type::TRANSACTION_END};
          gen_->data_.emplace_back(commit_timestamp, data);
        }
      } else {
        gen_->valid_ = false;
      }
    }

   private:
    DeltaGenerator *gen_;
    storage::Transaction transaction_;
    std::vector<storage::durability::WalDeltaData> data_;
  };

  using DataT = std::vector<std::pair<uint64_t, storage::durability::WalDeltaData>>;

  DeltaGenerator(const std::filesystem::path &data_directory, bool properties_on_edges, uint64_t seq_num)
      : uuid_(utils::GenerateUUID()),
        epoch_id_(utils::GenerateUUID()),
        seq_num_(seq_num),
        wal_file_(data_directory, uuid_, epoch_id_, {.properties_on_edges = properties_on_edges}, &mapper_, seq_num,
                  &file_retainer_) {}

  Transaction CreateTransaction() { return Transaction(this); }

  void ResetTransactionIds() {
    transaction_id_ = storage::kTransactionInitialId;
    timestamp_ = storage::kTimestampInitialId;
    valid_ = false;
  }

  void AppendOperation(storage::durability::StorageGlobalOperation operation, const std::string &label,
                       const std::set<std::string> properties = {}) {
    auto label_id = storage::LabelId::FromUint(mapper_.NameToId(label));
    std::set<storage::PropertyId> property_ids;
    for (const auto &property : properties) {
      property_ids.insert(storage::PropertyId::FromUint(mapper_.NameToId(property)));
    }
    wal_file_.AppendOperation(operation, label_id, property_ids, timestamp_);
    if (valid_) {
      UpdateStats(timestamp_, 1);
      storage::durability::WalDeltaData data;
      data.type = StorageGlobalOperationToWalDeltaDataType(operation);
      switch (operation) {
        case storage::durability::StorageGlobalOperation::LABEL_INDEX_CREATE:
        case storage::durability::StorageGlobalOperation::LABEL_INDEX_DROP:
          data.operation_label.label = label;
          break;
        case storage::durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE:
        case storage::durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP:
        case storage::durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE:
        case storage::durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP:
          data.operation_label_property.label = label;
          data.operation_label_property.property = *properties.begin();
        case storage::durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE:
        case storage::durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP:
          data.operation_label_properties.label = label;
          data.operation_label_properties.properties = properties;
      }
      data_.emplace_back(timestamp_, data);
    }
  }

  uint64_t GetPosition() { return wal_file_.GetSize(); }

  storage::durability::WalInfo GetInfo() {
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

  uint64_t transaction_id_{storage::kTransactionInitialId};
  uint64_t timestamp_{storage::kTimestampInitialId};
  uint64_t vertices_count_{0};
  std::list<storage::Vertex> vertices_;
  storage::NameIdMapper mapper_;

  storage::durability::WalFile wal_file_;

  DataT data_;

  uint64_t deltas_count_{0};
  uint64_t tx_from_{0};
  uint64_t tx_to_{0};
  uint64_t valid_{true};

  utils::FileRetainer file_retainer_;
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define TRANSACTION(append_transaction_end, ops) \
  {                                              \
    auto tx = gen.CreateTransaction();           \
    ops;                                         \
    tx.Finalize(append_transaction_end);         \
  }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define OPERATION(op, ...) gen.AppendOperation(storage::durability::StorageGlobalOperation::op, __VA_ARGS__)

void AssertWalInfoEqual(const storage::durability::WalInfo &a, const storage::durability::WalInfo &b) {
  ASSERT_EQ(a.uuid, b.uuid);
  ASSERT_EQ(a.epoch_id, b.epoch_id);
  ASSERT_EQ(a.seq_num, b.seq_num);
  ASSERT_EQ(a.from_timestamp, b.from_timestamp);
  ASSERT_EQ(a.to_timestamp, b.to_timestamp);
  ASSERT_EQ(a.num_deltas, b.num_deltas);
}

void AssertWalDataEqual(const DeltaGenerator::DataT &data, const std::filesystem::path &path) {
  auto info = storage::durability::ReadWalInfo(path);
  storage::durability::Decoder wal;
  wal.Initialize(path, storage::durability::kWalMagic);
  wal.SetPosition(info.offset_deltas);
  DeltaGenerator::DataT current;
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    auto timestamp = storage::durability::ReadWalDeltaHeader(&wal);
    current.emplace_back(timestamp, storage::durability::ReadWalDeltaData(&wal));
  }
  ASSERT_EQ(data.size(), current.size());
  ASSERT_EQ(data, current);
}

class WalFileTest : public ::testing::TestWithParam<bool> {
 public:
  WalFileTest() {}

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

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, WalFileTest, ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, WalFileTest, ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, EmptyFile) {
  { DeltaGenerator gen(storage_directory, GetParam(), 5); }
  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 0);
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_SIMPLE_TEST(name, ops)                                                                        \
  TEST_P(WalFileTest, name) {                                                                                  \
    storage::durability::WalInfo info;                                                                         \
    DeltaGenerator::DataT data;                                                                                \
                                                                                                               \
    {                                                                                                          \
      DeltaGenerator gen(storage_directory, GetParam(), 5);                                                    \
      ops;                                                                                                     \
      info = gen.GetInfo();                                                                                    \
      data = gen.GetData();                                                                                    \
    }                                                                                                          \
                                                                                                               \
    auto wal_files = GetFilesList();                                                                           \
    ASSERT_EQ(wal_files.size(), 1);                                                                            \
                                                                                                               \
    if (info.num_deltas == 0) {                                                                                \
      ASSERT_THROW(storage::durability::ReadWalInfo(wal_files.front()), storage::durability::RecoveryFailure); \
    } else {                                                                                                   \
      AssertWalInfoEqual(info, storage::durability::ReadWalInfo(wal_files.front()));                           \
      AssertWalDataEqual(data, wal_files.front());                                                             \
    }                                                                                                          \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithEnd, { TRANSACTION(true, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithoutEnd, { TRANSACTION(false, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(OperationSingle, { OPERATION(LABEL_INDEX_CREATE, "hello"); });

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
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_01, {
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_10, {
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation_11, {
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation0_0, {
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation0_1, {
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation1_0, {
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(false, { tx.CreateVertex(); });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation1_1, {
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
  TRANSACTION(true, { tx.CreateVertex(); });
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation00_, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation01_, {
  TRANSACTION(false, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation10_, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(false, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionsWithOperation11_, {
  TRANSACTION(true, { tx.CreateVertex(); });
  TRANSACTION(true, { tx.CreateVertex(); });
  OPERATION(LABEL_INDEX_CREATE, "hello");
});

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(AllTransactionOperationsWithEnd, {
  TRANSACTION(true, {
    auto vertex1 = tx.CreateVertex();
    auto vertex2 = tx.CreateVertex();
    tx.AddLabel(vertex1, "test");
    tx.AddLabel(vertex2, "hello");
    tx.SetProperty(vertex2, "hello", storage::PropertyValue("nandare"));
    tx.RemoveLabel(vertex1, "test");
    tx.SetProperty(vertex2, "hello", storage::PropertyValue(123));
    tx.SetProperty(vertex2, "hello", storage::PropertyValue());
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
    tx.SetProperty(vertex2, "hello", storage::PropertyValue("nandare"));
    tx.RemoveLabel(vertex1, "test");
    tx.SetProperty(vertex2, "hello", storage::PropertyValue(123));
    tx.SetProperty(vertex2, "hello", storage::PropertyValue());
    tx.DeleteVertex(vertex1);
  });
});
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(AllGlobalOperations, {
  OPERATION(LABEL_INDEX_CREATE, "hello");
  OPERATION(LABEL_INDEX_DROP, "hello");
  OPERATION(LABEL_PROPERTY_INDEX_CREATE, "hello", {"world"});
  OPERATION(LABEL_PROPERTY_INDEX_DROP, "hello", {"world"});
  OPERATION(EXISTENCE_CONSTRAINT_CREATE, "hello", {"world"});
  OPERATION(EXISTENCE_CONSTRAINT_DROP, "hello", {"world"});
  OPERATION(UNIQUE_CONSTRAINT_CREATE, "hello", {"world", "and", "universe"});
  OPERATION(UNIQUE_CONSTRAINT_DROP, "hello", {"world", "and", "universe"});
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
  storage::durability::WalInfo info;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    info = gen.GetInfo();
  }

  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 1);
  const auto &wal_file = wal_files.front();

  auto final_info = storage::durability::ReadWalInfo(wal_file);
  AssertWalInfoEqual(info, final_info);

  size_t i = 0;
  for (auto marker : storage::durability::kMarkersAll) {
    if (marker == storage::durability::Marker::SECTION_DELTA) continue;
    auto current_file = storage_directory / fmt::format("temporary_{}", i);
    ASSERT_TRUE(std::filesystem::copy_file(wal_file, current_file));
    utils::OutputFile file;
    file.Open(current_file, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    file.SetPosition(utils::OutputFile::Position::SET, final_info.offset_deltas);
    auto value = static_cast<uint8_t>(marker);
    file.Write(&value, sizeof(value));
    file.Sync();
    file.Close();
    ASSERT_THROW(storage::durability::ReadWalInfo(current_file), storage::durability::RecoveryFailure);
    ++i;
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, PartialData) {
  std::vector<std::pair<uint64_t, storage::durability::WalInfo>> infos;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    TRANSACTION(true, {
      auto vertex = tx.CreateVertex();
      tx.AddLabel(vertex, "hello");
    });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    OPERATION(LABEL_PROPERTY_INDEX_CREATE, "hello", {"world"});
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    TRANSACTION(true, {
      auto vertex1 = tx.CreateVertex();
      auto vertex2 = tx.CreateVertex();
      tx.AddLabel(vertex1, "test");
      tx.AddLabel(vertex2, "hello");
      tx.SetProperty(vertex2, "hello", storage::PropertyValue("nandare"));
      tx.RemoveLabel(vertex1, "test");
      tx.SetProperty(vertex2, "hello", storage::PropertyValue(123));
      tx.SetProperty(vertex2, "hello", storage::PropertyValue());
      tx.DeleteVertex(vertex1);
    });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
  }

  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 1);
  const auto &wal_file = wal_files.front();

  AssertWalInfoEqual(infos.back().second, storage::durability::ReadWalInfo(wal_file));

  auto current_file = storage_directory / "temporary";
  utils::InputFile infile;
  infile.Open(wal_file);

  uint64_t pos = 0;
  for (size_t i = 0; i < infile.GetSize(); ++i) {
    if (i < infos.front().first) {
      ASSERT_THROW(storage::durability::ReadWalInfo(current_file), storage::durability::RecoveryFailure);
    } else {
      if (i >= infos[pos + 1].first) ++pos;
      AssertWalInfoEqual(infos[pos].second, storage::durability::ReadWalInfo(current_file));
    }
    {
      utils::OutputFile outfile;
      outfile.Open(current_file, utils::OutputFile::Mode::APPEND_TO_EXISTING);
      uint8_t value;
      ASSERT_TRUE(infile.Read(&value, sizeof(value)));
      outfile.Write(&value, sizeof(value));
      outfile.Sync();
      outfile.Close();
    }
  }
  ASSERT_EQ(pos, infos.size() - 2);
  AssertWalInfoEqual(infos[infos.size() - 1].second, storage::durability::ReadWalInfo(current_file));
}
