#pragma once

#include <mutex>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/common/types/types.hpp"
#include "utils/algorithm.hpp"

namespace storage {

/** SingleNode implementation of ConcurrentIdMapper. */
template <typename TId>
class ConcurrentIdMapper {
  using StorageT = typename TId::IdT;

 public:
  ConcurrentIdMapper() = default;
  explicit ConcurrentIdMapper(
      const std::vector<std::string> &properties_on_disk)
      : properties_on_disk_(properties_on_disk) {}

  TId value_to_id(const std::string &value) {
    auto value_to_id_acc = value_to_id_.access();
    auto found = value_to_id_acc.find(value);
    TId inserted_id(0);
    if (found == value_to_id_acc.end()) {
      StorageT new_id = id_.fetch_add(1);
      // After we tried to insert value with our id we either got our id, or the
      // id created by the thread which succesfully inserted (value, id) pair
      // because that's ConcurrentMap's behaviour
      if (std::is_same<TId, Property>::value)
        inserted_id =
            value_to_id_acc.insert(value, TId(new_id, PropertyLocation(value)))
                .first->second;
      else
        inserted_id = value_to_id_acc.insert(value, TId(new_id)).first->second;
    } else {
      inserted_id = found->second;
    }
    auto id_to_value_acc = id_to_value_.access();
    // We have to try to insert the inserted_id and value even if we are not the
    // one who assigned id because we have to make sure that after this method
    // returns that both mappings between id->value and value->id exist.
    id_to_value_acc.insert(inserted_id, value);
    return inserted_id;
  }

  const std::string &id_to_value(const TId &id) {
    auto id_to_value_acc = id_to_value_.access();
    auto result = id_to_value_acc.find(id);
    DCHECK(result != id_to_value_acc.end());
    return result->second;
  }

 private:
  ConcurrentMap<std::string, TId> value_to_id_;
  ConcurrentMap<TId, std::string> id_to_value_;
  std::atomic<StorageT> id_{0};
  std::vector<std::string> properties_on_disk_;
  std::mutex mutex_;

  Location PropertyLocation(const std::string &name) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (utils::Contains(properties_on_disk_, name)) return Location::Disk;
    return Location::Memory;
  }
};
}  // namespace storage
