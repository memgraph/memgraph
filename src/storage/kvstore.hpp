#pragma once

#include <experimental/filesystem>
#include <experimental/optional>
#include <memory>
#include <string>

#include "utils/exceptions.hpp"

namespace storage {

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
  KVStore() = delete;

  /**
   * @param storage Path to a directory where the data is persisted.
   *
   * NOTE: Don't instantiate more instances of a KVStore with the same
   *       storage directory because that will lead to undefined behaviour.
   */
  explicit KVStore(std::experimental::filesystem::path storage);

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
   * Deletes the key and corresponding value from storage.
   *
   * @param key
   *
   * @return True on success, false on error. The return value is
   *         true if the key doesn't exist and underlying storage
   *         didn't encounter any error.
   */
  bool Delete(const std::string &key);

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
  bool DeletePrefix(const std::string &prefix = "");

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
  size_t Size(const std::string &prefix = "");

  /**
   * Custom prefix-based iterator over kvstore.
   *
   * It filters all (key, value) pairs where the key has a certain prefix
   * and behaves as if all of those pairs are stored in a single iterable
   * collection of std::pair<std::string, std::string>.
   */
  class iterator final
      : public std::iterator<
            std::input_iterator_tag,                      // iterator_category
            std::pair<std::string, std::string>,          // value_type
            long,                                         // difference_type
            const std::pair<std::string, std::string> *,  // pointer
            const std::pair<std::string, std::string> &   // reference
            > {
   public:
    explicit iterator(const KVStore *kvstore, const std::string &prefix = "",
                      bool at_end = false);

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

  iterator begin(const std::string &prefix = "") {
    return iterator(this, prefix);
  }

  iterator end(const std::string &prefix = "") {
    return iterator(this, prefix, true);
  }

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;
};

}  // namespace storage
