#include "storage/kvstore.hpp"

#include "durability/paths.hpp"
#include "utils/file.hpp"

namespace storage {

KVStore::KVStore(fs::path storage) : storage_(storage) {
  if (!utils::EnsureDir(storage))
    throw KVStoreError("Folder for the key-value store " + storage.string() +
                       " couldn't be initialized!");
  options_.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options_, storage.c_str(), &db);
  if (!s.ok())
    throw KVStoreError("RocksDB couldn't be initialized inside " +
                       storage.string() + "!");
  db_.reset(db);
}

bool KVStore::Put(const std::string &key, const std::string &value) {
  auto s = db_->Put(rocksdb::WriteOptions(), key.c_str(), value.c_str());
  if (!s.ok()) return false;
  return true;
}

std::experimental::optional<std::string> KVStore::Get(
    const std::string &key) const noexcept {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), key.c_str(), &value);
  if (!s.ok()) return std::experimental::nullopt;
  return value;
}

bool KVStore::Delete(const std::string &key) {
  auto s = db_->SingleDelete(rocksdb::WriteOptions(), key.c_str());
  if (!s.ok()) return false;
  return true;
}

}  // namespace storage
