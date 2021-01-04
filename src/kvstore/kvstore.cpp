#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "kvstore/kvstore.hpp"
#include "kvstore/replication/rpc.hpp"
#include "utils/file.hpp"

namespace kvstore {

DEFINE_bool(main, false, "Set to true to be the main");
DEFINE_bool(replica, false, "Set to true to be the replica");

struct KVStore::impl {
  std::filesystem::path storage;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::Options options;
};

KVStore::KVStore(std::filesystem::path storage)
    : pimpl_(std::make_unique<impl>()) {
  if (FLAGS_main) {
    DLOG(INFO) << "SETTING CLIENT FOR AUTH";
    rpc_context_.emplace();
    rpc_client_.emplace(io::network::Endpoint("127.0.0.1", 10000),
                        &*rpc_context_);
  } else if (FLAGS_replica) {
    rpc_server_context_.emplace();
    rpc_server_.emplace(io::network::Endpoint("127.0.0.1", 10000),
                        &*rpc_server_context_);
    rpc_server_->Register<AppendKvstoreRpc>([this](auto *req_reader,
                                                   auto *res_builder) {
      AppendKvstoreReq req;
      slk::Load(&req, req_reader);
      DLOG(INFO) << "Received AppendKvstoreRpc";
      size_t updates_num;
      slk::Load(&updates_num, req_reader);
      for (size_t i = 0; i < updates_num; ++i) {
        size_t update_size;
        slk::Load(&update_size, req_reader);
        std::vector<uint8_t> update_data(update_size);
        req_reader->Load(update_data.data(), update_size);
        rocksdb::WriteBatch write_batch(std::string(
            reinterpret_cast<const char *>(update_data.data()), update_size));
        pimpl_->db->Write(rocksdb::WriteOptions(), &write_batch);
      }
      const auto next_sequence_num = pimpl_->db->GetLatestSequenceNumber();
      AppendKvstoreRes res{true, next_sequence_num};
      slk::Save(res, res_builder);
    });
    rpc_server_->Start();
  }
  pimpl_->storage = storage;
  if (!utils::EnsureDir(pimpl_->storage))
    throw KVStoreError("Folder for the key-value store " +
                       pimpl_->storage.string() + " couldn't be initialized!");
  pimpl_->options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(pimpl_->options, storage.c_str(), &db);
  if (!s.ok())
    throw KVStoreError("RocksDB couldn't be initialized inside " +
                       storage.string() + " -- " + std::string(s.ToString()));
  pimpl_->db.reset(db);
}

KVStore::~KVStore() {
  if (rpc_server_) {
    rpc_server_->Shutdown();
    rpc_server_->AwaitShutdown();
  }
}

KVStore::KVStore(KVStore &&other) { pimpl_ = std::move(other.pimpl_); }

KVStore &KVStore::operator=(KVStore &&other) {
  pimpl_ = std::move(other.pimpl_);
  return *this;
}

bool KVStore::Put(const std::string &key, const std::string &value) {
  auto s = pimpl_->db->Put(rocksdb::WriteOptions(), key, value);
  return s.ok();
}

bool KVStore::PutMultiple(const std::map<std::string, std::string> &items) {
  rocksdb::WriteBatch batch;
  for (const auto &item : items) {
    batch.Put(item.first, item.second);
  }
  auto s = pimpl_->db->Write(rocksdb::WriteOptions(), &batch);
  return s.ok();
}

std::optional<std::string> KVStore::Get(const std::string &key) const noexcept {
  std::string value;
  auto s = pimpl_->db->Get(rocksdb::ReadOptions(), key, &value);
  if (!s.ok()) return std::nullopt;
  return value;
}

bool KVStore::Delete(const std::string &key) {
  auto s = pimpl_->db->Delete(rocksdb::WriteOptions(), key);
  return s.ok();
}

bool KVStore::DeleteMultiple(const std::vector<std::string> &keys) {
  rocksdb::WriteBatch batch;
  for (const auto &key : keys) {
    batch.Delete(key);
  }
  auto s = pimpl_->db->Write(rocksdb::WriteOptions(), &batch);
  return s.ok();
}

bool KVStore::DeletePrefix(const std::string &prefix) {
  std::unique_ptr<rocksdb::Iterator> iter = std::unique_ptr<rocksdb::Iterator>(
      pimpl_->db->NewIterator(rocksdb::ReadOptions()));
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix);
       iter->Next()) {
    if (!pimpl_->db->Delete(rocksdb::WriteOptions(), iter->key()).ok())
      return false;
  }
  return true;
}

bool KVStore::PutAndDeleteMultiple(
    const std::map<std::string, std::string> &items,
    const std::vector<std::string> &keys) {
  rocksdb::WriteBatch batch;
  for (const auto &item : items) {
    batch.Put(item.first, item.second);
  }
  for (const auto &key : keys) {
    batch.Delete(key);
  }
  auto s = pimpl_->db->Write(rocksdb::WriteOptions(), &batch);
  return s.ok();
}

void KVStore::Replicate() {
  auto stream = rpc_client_->Stream<AppendKvstoreRpc>();
  auto *builder = stream.GetBuilder();

  std::vector<std::vector<std::uint8_t>> updates;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto status = pimpl_->db->GetUpdatesSince(next_sequence_num_, &iter);
  if (status.ok()) {
    for (; iter && iter->Valid(); iter->Next()) {
      auto result = iter->GetBatch();
      const auto &str = result.writeBatchPtr->Data();
      std::vector<std::uint8_t> raw_data;
      raw_data.resize(str.size());
      memcpy(raw_data.data(), str.data(), str.size());
      updates.emplace_back(std::move(raw_data));
    }
  }

  slk::Save(updates.size(), builder);
  for (const auto &update : updates) {
    slk::Save(update.size(), builder);
    builder->Save(update.data(), update.size());
  }

  const auto response = stream.AwaitResponse();
  next_sequence_num_ = response.next_sequence_num;
};

// iterator

struct KVStore::iterator::impl {
  const KVStore *kvstore;
  std::string prefix;
  std::unique_ptr<rocksdb::Iterator> it;
  std::pair<std::string, std::string> disk_prop;
};

KVStore::iterator::iterator(const KVStore *kvstore, const std::string &prefix,
                            bool at_end)
    : pimpl_(std::make_unique<impl>()) {
  pimpl_->kvstore = kvstore;
  pimpl_->prefix = prefix;
  pimpl_->it = std::unique_ptr<rocksdb::Iterator>(
      pimpl_->kvstore->pimpl_->db->NewIterator(rocksdb::ReadOptions()));
  pimpl_->it->Seek(pimpl_->prefix);
  if (!pimpl_->it->Valid() || !pimpl_->it->key().starts_with(pimpl_->prefix) ||
      at_end)
    pimpl_->it = nullptr;
}

KVStore::iterator::iterator(KVStore::iterator &&other) {
  pimpl_ = std::move(other.pimpl_);
}

KVStore::iterator::~iterator() {}

KVStore::iterator &KVStore::iterator::operator=(KVStore::iterator &&other) {
  pimpl_ = std::move(other.pimpl_);
  return *this;
}

KVStore::iterator &KVStore::iterator::operator++() {
  pimpl_->it->Next();
  if (!pimpl_->it->Valid() || !pimpl_->it->key().starts_with(pimpl_->prefix))
    pimpl_->it = nullptr;
  return *this;
}

bool KVStore::iterator::operator==(const iterator &other) const {
  return pimpl_->kvstore == other.pimpl_->kvstore &&
         pimpl_->prefix == other.pimpl_->prefix &&
         pimpl_->it == other.pimpl_->it;
}

bool KVStore::iterator::operator!=(const iterator &other) const {
  return !(*this == other);
}

KVStore::iterator::reference KVStore::iterator::operator*() {
  pimpl_->disk_prop = {pimpl_->it->key().ToString(),
                       pimpl_->it->value().ToString()};
  return pimpl_->disk_prop;
}

KVStore::iterator::pointer KVStore::iterator::operator->() { return &**this; }

void KVStore::iterator::SetInvalid() { pimpl_->it = nullptr; }

bool KVStore::iterator::IsValid() { return pimpl_->it != nullptr; }

// TODO(ipaljak) The complexity of the size function should be at most
//               logarithmic.
size_t KVStore::Size(const std::string &prefix) {
  size_t size = 0;
  for (auto it = this->begin(prefix); it != this->end(prefix); ++it) ++size;
  return size;
}

bool KVStore::CompactRange(const std::string &begin_prefix,
                           const std::string &end_prefix) {
  rocksdb::CompactRangeOptions options;
  rocksdb::Slice begin(begin_prefix);
  rocksdb::Slice end(end_prefix);
  auto s = pimpl_->db->CompactRange(options, &begin, &end);
  return s.ok();
}

}  // namespace kvstore
