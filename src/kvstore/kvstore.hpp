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

#pragma once

#include <rocksdb/options.h>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"

#include <rocksdb/iterator.h>

namespace memgraph::kvstore {

class KVStoreError : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(KVStoreError)
};

/**
 * Abstraction used to manage key-value pairs. The underlying implementation
 * guarantees thread safety and durability properties.
 */
class KVStore final {
 public:
  KVStore() = delete;

  /**
   * @param storage Path to a directory where the data is persisted.
   *
   * NOTE: Don't instantiate more instances of a KVStore with the same
   *       storage directory because that will lead to undefined behaviour.
   */
  explicit KVStore(std::filesystem::path storage);
  explicit KVStore(std::filesystem::path storage, rocksdb::Options db_options);

  KVStore(const KVStore &other) = delete;
  KVStore(KVStore &&other);

  KVStore &operator=(const KVStore &other) = delete;
  KVStore &operator=(KVStore &&other);

  ~KVStore();

  /**
   * Store value under the given key.
   *
   * @param key
   * @param value
   *
   * @return true if the value has been successfully stored.
   *         In case of any error false is going to be returned.
   */
  bool Put(std::string_view key, std::string_view value, rocksdb::WriteOptions options = {});

  /**
   * Store values under the given keys.
   *
   * @param items
   *
   * @return true if the items have been successfully stored.
   *         In case of any error false is going to be returned.
   */
  bool PutMultiple(const std::map<std::string, std::string> &items, rocksdb::WriteOptions options = {});

  /**
   * Retrieve value for the given key.
   *
   * @param key
   *
   * @return Value for the given key. std::nullopt in case of any error
   *         OR the value doesn't exist.
   */
  std::optional<std::string> Get(std::string_view key, rocksdb::ReadOptions options = {}) const noexcept;

  rocksdb::Iterator *GetItr(std::string_view prefix = "", rocksdb::ReadOptions options = {});

  std::map<std::string, std::string> GetMultiple(std::vector<rocksdb::Slice> keys,
                                                 rocksdb::ReadOptions options) const noexcept;

  /**
   * Deletes the key and corresponding value from storage.
   *
   * @param key
   *
   * @return True on success, false on error. The return value is
   *         true if the key doesn't exist and underlying storage
   *         didn't encounter any error.
   */
  bool Delete(std::string_view key, rocksdb::WriteOptions options = {});

  /**
   * Deletes the keys and corresponding values from storage.
   *
   * @param keys
   *
   * @return True on success, false on error. The return value is
   *         true if the keys don't exist and underlying storage
   *         didn't encounter any error.
   */
  bool DeleteMultiple(const std::vector<std::string> &keys, rocksdb::WriteOptions options = {});

  /**
   * Delete all (key, value) pairs where key begins with a given prefix.
   *
   * @param prefix - prefix of the keys in (key, value) pairs to be deleted.
   *                 This parameter is optional and is empty by default.
   *
   * @return True on success, false on error. The return value is
   *         true if the key doesn't exist and underlying storage
   *         didn't encounter any error.
   */
  bool DeletePrefix(const std::string &prefix = "", rocksdb::WriteOptions options = {},
                    rocksdb::ReadOptions read_options = {});

  /**
   * Store values under the given keys and delete the keys.
   *
   * @param items
   * @param keys
   *
   * @return true if the items have been successfully stored and deleted.
   *         In case of any error false is going to be returned.
   */
  bool PutAndDeleteMultiple(const std::map<std::string, std::string> &items, const std::vector<std::string> &keys,
                            rocksdb::WriteOptions options = {});

  /**
   * Returns total number of stored (key, value) pairs. The function takes an
   * optional prefix parameter used for filtering keys that start with that
   * prefix.
   *
   * @param prefix - prefix on which the keys should be filtered. This parameter
   *                 is optional and is empty by default.
   *
   * @return - number of stored pairs.
   */
  size_t Size(const std::string &prefix = "", rocksdb::ReadOptions options = {}) const;

  /**
   * Compact the underlying storage for the key range [begin_prefix,
   * end_prefix].
   * The actual compaction interval might be superset of
   * [begin_prefix, end_prefix].
   *
   * @param begin_prefix - Compaction interval start.
   * @param end_prefix   - Compaction interval end.
   *
   * @return - true if the compaction finished successfully, false otherwise.
   */
  bool CompactRange(const std::string &begin_prefix, const std::string &end_prefix);

  /**
   * Custom prefix-based iterator over kvstore.
   *
   * It filters all (key, value) pairs where the key has a certain prefix
   * and behaves as if all of those pairs are stored in a single iterable
   * collection of std::pair<std::string, std::string>.
   */
  class iterator final {
   public:
    using iterator_concept [[maybe_unused]] = std::input_iterator_tag;
    using value_type = std::pair<std::string, std::string>;
    using difference_type = long;
    using pointer = const std::pair<std::string, std::string> *;
    using reference = const std::pair<std::string, std::string> &;

    explicit iterator(const KVStore *kvstore, const std::string &prefix = "", bool at_end = false,
                      rocksdb::ReadOptions options = {});

    iterator(const iterator &other) = delete;

    iterator(iterator &&other);

    ~iterator();

    iterator &operator=(iterator &&other);

    iterator &operator=(const iterator &other) = delete;

    iterator &operator++();

    bool operator==(const iterator &other) const;

    bool operator!=(const iterator &other) const;

    reference operator*();

    pointer operator->();

    void SetInvalid();

    bool IsValid();

   private:
    struct impl;
    std::unique_ptr<impl> pimpl_;
  };

  iterator begin(const std::string &prefix = "", rocksdb::ReadOptions options = {}) const {
    return iterator(this, prefix, false, options);
  }

  iterator end(const std::string &prefix = "", rocksdb::ReadOptions options = {}) const {
    return iterator(this, prefix, true, options);
  }

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;
};

}  // namespace memgraph::kvstore
