#pragma once

#include <experimental/optional>
#include <map>
#include <string>

#include <glog/logging.h>

// TODO(ipaljak): replace with the real implementation

namespace storage {

class KVStore {
 public:
  explicit KVStore(const std::string &) {}

  bool Put(const std::string &key, const std::string &value) {
    VLOG(31) << "PUT: " << key << " : " << value;
    storage_[key] = value;
    return true;
  }

  std::experimental::optional<std::string> Get(const std::string &key) const {
    VLOG(31) << "GET: " << key;
    auto it = storage_.find(key);
    if (it == storage_.end()) return std::experimental::nullopt;
    return it->second;
  }

  bool Delete(const std::string &key) {
    VLOG(31) << "DELETE: " << key;
    storage_.erase(key);
    return true;
  }

 private:
  std::map<std::string, std::string> storage_;
};

} // namespace storage
