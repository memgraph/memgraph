#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <string_view>

#include "storage/v2/durability.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/file.hpp"
#include "utils/uuid.hpp"

// This class mimics the internals of the storage to generate the deltas.
class DeltaGenerator final {
 public:
  class Transaction final {
   private:
    friend class DeltaGenerator;

    explicit Transaction(DeltaGenerator *gen)
        : gen_(gen), transaction_(gen->transaction_id_++, gen->timestamp_++) {}

   public:
    storage::Vertex *CreateVertex() {
      auto gid = storage::Gid::FromUint(gen_->vertices_count_++);
      auto delta = storage::CreateDeleteObjectDelta(&transaction_);
      auto &it = gen_->vertices_.emplace_back(gid, delta);
      delta->prev.Set(&it);
      return &it;
    }

    void DeleteVertex(storage::Vertex *vertex) {
      storage::CreateAndLinkDelta(&transaction_, &*vertex,
                                  storage::Delta::RecreateObjectTag());
    }

    void AddLabel(storage::Vertex *vertex, const std::string &label) {
      auto label_id = storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.push_back(label_id);
      storage::CreateAndLinkDelta(&transaction_, &*vertex,
                                  storage::Delta::RemoveLabelTag(), label_id);
    }

    void RemoveLabel(storage::Vertex *vertex, const std::string &label) {
      auto label_id = storage::LabelId::FromUint(gen_->mapper_.NameToId(label));
      vertex->labels.erase(
          std::find(vertex->labels.begin(), vertex->labels.end(), label_id));
      storage::CreateAndLinkDelta(&transaction_, &*vertex,
                                  storage::Delta::AddLabelTag(), label_id);
    }

    void SetProperty(storage::Vertex *vertex, const std::string &property,
                     const storage::PropertyValue &value) {
      auto property_id =
          storage::PropertyId::FromUint(gen_->mapper_.NameToId(property));
      auto &props = vertex->properties;
      auto it = props.find(property_id);
      if (it == props.end()) {
        storage::CreateAndLinkDelta(&transaction_, &*vertex,
                                    storage::Delta::SetPropertyTag(),
                                    property_id, storage::PropertyValue());
        if (!value.IsNull()) {
          props.emplace(property_id, value);
        }
      } else {
        storage::CreateAndLinkDelta(&transaction_, &*vertex,
                                    storage::Delta::SetPropertyTag(),
                                    property_id, it->second);
        if (!value.IsNull()) {
          it->second = value;
        } else {
          props.erase(it);
        }
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
          gen_->wal_file_.AppendDelta(delta, owner.vertex, commit_timestamp);
        } else if (owner.type == storage::PreviousPtr::Type::EDGE) {
          gen_->wal_file_.AppendDelta(delta, owner.edge, commit_timestamp);
        } else {
          LOG(FATAL) << "Invalid delta owner!";
        }
      }
      if (append_transaction_end) {
        gen_->wal_file_.AppendTransactionEnd(commit_timestamp);
        gen_->UpdateStats(commit_timestamp, transaction_.deltas.size() + 1);
      } else {
        gen_->valid_ = false;
      }
    }

   private:
    DeltaGenerator *gen_;
    storage::Transaction transaction_;
  };

  DeltaGenerator(const std::filesystem::path &data_directory,
                 bool properties_on_edges, uint64_t seq_num)
      : uuid_(utils::GenerateUUID()),
        seq_num_(seq_num),
        wal_file_(data_directory, uuid_,
                  {.properties_on_edges = properties_on_edges}, &mapper_,
                  seq_num) {}

  Transaction CreateTransaction() { return Transaction(this); }

  void ResetTransactionIds() {
    transaction_id_ = storage::kTransactionInitialId;
    timestamp_ = storage::kTimestampInitialId;
    valid_ = false;
  }

  void AppendOperation(storage::StorageGlobalOperation operation,
                       const std::string &label,
                       std::optional<std::string> property = std::nullopt) {
    auto label_id = storage::LabelId::FromUint(mapper_.NameToId(label));
    std::optional<storage::PropertyId> property_id;
    if (property) {
      property_id = storage::PropertyId::FromUint(mapper_.NameToId(*property));
    }
    wal_file_.AppendOperation(operation, label_id, property_id, timestamp_);
    UpdateStats(timestamp_, 1);
  }

  uint64_t GetPosition() { return wal_file_.GetSize(); }

  storage::WalInfo GetInfo() {
    return {.offset_metadata = 0,
            .offset_deltas = 0,
            .uuid = uuid_,
            .seq_num = seq_num_,
            .from_timestamp = tx_from_,
            .to_timestamp = tx_to_,
            .num_deltas = deltas_count_};
  }

 private:
  void UpdateStats(uint64_t timestamp, uint64_t count) {
    if (!valid_) return;
    if (deltas_count_ == 0) {
      tx_from_ = timestamp;
    }
    tx_to_ = timestamp;
    deltas_count_ += count;
  }

  std::string uuid_;
  uint64_t seq_num_;

  uint64_t transaction_id_{storage::kTransactionInitialId};
  uint64_t timestamp_{storage::kTimestampInitialId};
  uint64_t vertices_count_{0};
  std::list<storage::Vertex> vertices_;
  storage::NameIdMapper mapper_;

  storage::WalFile wal_file_;

  uint64_t deltas_count_{0};
  uint64_t tx_from_{0};
  uint64_t tx_to_{0};
  uint64_t valid_{true};
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define TRANSACTION(append_transaction_end, ops) \
  {                                              \
    auto tx = gen.CreateTransaction();           \
    ops;                                         \
    tx.Finalize(append_transaction_end);         \
  }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define OPERATION(op, ...) \
  gen.AppendOperation(storage::StorageGlobalOperation::op, __VA_ARGS__)

void AssertWalInfoEqual(const storage::WalInfo &a, const storage::WalInfo &b) {
  ASSERT_EQ(a.uuid, b.uuid);
  ASSERT_EQ(a.seq_num, b.seq_num);
  ASSERT_EQ(a.from_timestamp, b.from_timestamp);
  ASSERT_EQ(a.to_timestamp, b.to_timestamp);
  ASSERT_EQ(a.num_deltas, b.num_deltas);
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

  std::filesystem::path storage_directory{
      std::filesystem::temp_directory_path() /
      "MG_test_unit_storage_v2_wal_file"};

 private:
  void Clear() {
    if (!std::filesystem::exists(storage_directory)) return;
    std::filesystem::remove_all(storage_directory);
  }
};

INSTANTIATE_TEST_CASE_P(EdgesWithProperties, WalFileTest,
                        ::testing::Values(true));
INSTANTIATE_TEST_CASE_P(EdgesWithoutProperties, WalFileTest,
                        ::testing::Values(false));

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, EmptyFile) {
  { DeltaGenerator gen(storage_directory, GetParam(), 5); }
  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 0);
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_SIMPLE_TEST(name, ops)                                  \
  TEST_P(WalFileTest, name) {                                            \
    storage::WalInfo info;                                               \
                                                                         \
    {                                                                    \
      DeltaGenerator gen(storage_directory, GetParam(), 5);              \
      ops;                                                               \
      info = gen.GetInfo();                                              \
    }                                                                    \
                                                                         \
    auto wal_files = GetFilesList();                                     \
    ASSERT_EQ(wal_files.size(), 1);                                      \
                                                                         \
    if (info.num_deltas == 0) {                                          \
      ASSERT_THROW(storage::ReadWalInfo(wal_files.front()),              \
                   storage::RecoveryFailure);                            \
    } else {                                                             \
      AssertWalInfoEqual(info, storage::ReadWalInfo(wal_files.front())); \
    }                                                                    \
  }

// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithEnd,
                     { TRANSACTION(true, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(TransactionWithoutEnd,
                     { TRANSACTION(false, { tx.CreateVertex(); }); });
// NOLINTNEXTLINE(hicpp-special-member-functions)
GENERATE_SIMPLE_TEST(OperationSingle,
                     { OPERATION(LABEL_INDEX_CREATE, "hello"); });

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
  OPERATION(LABEL_PROPERTY_INDEX_CREATE, "hello", "world");
  OPERATION(LABEL_PROPERTY_INDEX_DROP, "hello", "world");
  OPERATION(EXISTENCE_CONSTRAINT_CREATE, "hello", "world");
  OPERATION(EXISTENCE_CONSTRAINT_DROP, "hello", "world");
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
  storage::WalInfo info;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    info = gen.GetInfo();
  }

  auto wal_files = GetFilesList();
  ASSERT_EQ(wal_files.size(), 1);
  const auto &wal_file = wal_files.front();

  auto final_info = storage::ReadWalInfo(wal_file);
  AssertWalInfoEqual(info, final_info);

  size_t i = 0;
  for (auto marker : storage::kMarkersAll) {
    if (marker == storage::Marker::SECTION_DELTA) continue;
    auto current_file = storage_directory / fmt::format("temporary_{}", i);
    ASSERT_TRUE(std::filesystem::copy_file(wal_file, current_file));
    utils::OutputFile file;
    file.Open(current_file, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    file.SetPosition(utils::OutputFile::Position::SET,
                     final_info.offset_deltas);
    auto value = static_cast<uint8_t>(marker);
    file.Write(&value, sizeof(value));
    file.Sync();
    file.Close();
    ASSERT_THROW(storage::ReadWalInfo(current_file), storage::RecoveryFailure);
    ++i;
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(WalFileTest, PartialData) {
  std::vector<std::pair<uint64_t, storage::WalInfo>> infos;

  {
    DeltaGenerator gen(storage_directory, GetParam(), 5);
    TRANSACTION(true, { tx.CreateVertex(); });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    TRANSACTION(true, {
      auto vertex = tx.CreateVertex();
      tx.AddLabel(vertex, "hello");
    });
    infos.emplace_back(gen.GetPosition(), gen.GetInfo());
    OPERATION(LABEL_PROPERTY_INDEX_CREATE, "hello", "world");
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

  AssertWalInfoEqual(infos.back().second, storage::ReadWalInfo(wal_file));

  auto current_file = storage_directory / "temporary";
  utils::InputFile infile;
  infile.Open(wal_file);

  uint64_t pos = 0;
  for (size_t i = 0; i < infile.GetSize(); ++i) {
    if (i < infos.front().first) {
      ASSERT_THROW(storage::ReadWalInfo(current_file),
                   storage::RecoveryFailure);
    } else {
      if (i >= infos[pos + 1].first) ++pos;
      AssertWalInfoEqual(infos[pos].second, storage::ReadWalInfo(current_file));
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
  AssertWalInfoEqual(infos[infos.size() - 1].second,
                     storage::ReadWalInfo(current_file));
}
