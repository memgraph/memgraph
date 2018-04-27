#pragma once

#include <experimental/filesystem>
#include <experimental/optional>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "utils/exceptions.hpp"

namespace storage {

namespace fs = std::experimental::filesystem;

class KVStoreError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * Abstraction used to manage key-value pairs. The underlying implementation
 * guarantees thread safety and durability properties.
 */
class KVStore final {
 public:
  /**
   * @param storage Path to a directory where the data is persisted.
   *
   * NOTE: Don't instantiate more instances of a KVStore with the same
   *       storage directory because that will lead to undefined behaviour.
   */
  explicit KVStore(fs::path storage);

  /**
   * Store value under the given key.
   *
   * @param key
   * @param value
   *
   * @return true if the value has been successfully stored.
   *         In case of any error false is going to be returned.
   */
  bool Put(const std::string &key, const std::string &value);

  /**
   * Retrieve value for the given key.
   *
   * @param key
   *
   * @return Value for the given key. std::nullopt in case of any error
   *         OR the value doesn't exist.
   */
  std::experimental::optional<std::string> Get(const std::string &key) const
      noexcept;

  /**
   * Delete value under the given key.
   *
   * @param key
   *
   * @return True on success, false on error. The return value is
   *         true if the key doesn't exist and underlying storage
   *         didn't encounter any error.
   */
  bool Delete(const std::string &key);

 private:
  fs::path storage_;
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::Options options_;
};

}  // namespace storage
