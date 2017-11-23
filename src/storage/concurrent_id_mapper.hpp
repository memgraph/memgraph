#pragma once

#include "glog/logging.h"

#include "data_structures/concurrent/concurrent_map.hpp"

/**
 * @brief Implements injection between values and ids. Safe to use
 * concurrently.
 * @TParam TIds - Id type which to use
 * @TParam TIdPrimitive - Primitive type which defines storage size (uint16_t,
 * uint32_t, etc.)
 * @TParam TRecord - Value type for which to define bijection
 */
template <typename TId, typename TIdPrimitive, typename TValue>
class ConcurrentIdMapper {
 public:
  /**
   * Thread safe insert value and get a unique id for it. Calling this method
   * with the same value from any number of threads will always return the same
   * id, i.e. mapping between values and ids will always be consistent.
   */
  TId insert_value(const TValue &value) {
    auto value_to_id_acc = value_to_id_.access();
    auto found = value_to_id_acc.find(value);
    TId inserted_id(0);
    if (found == value_to_id_acc.end()) {
      TIdPrimitive new_id = id_.fetch_add(1);
      DCHECK(new_id < std::numeric_limits<TIdPrimitive>::max())
          << "Number of used ids overflowed our container";
      auto insert_result = value_to_id_acc.insert(value, TId(new_id));
      // After we tried to insert value with our id we either got our id, or the
      // id created by the thread which succesfully inserted (value, id) pair
      inserted_id = insert_result.first->second;
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

  const TValue &value_by_id(const TId &id) const {
    const auto id_to_value_acc = id_to_value_.access();
    auto result = id_to_value_acc.find(id);
    DCHECK(result != id_to_value_acc.end());
    return result->second;
  }

 private:
  ConcurrentMap<TId, TValue> id_to_value_;
  ConcurrentMap<TValue, TId> value_to_id_;
  std::atomic<TIdPrimitive> id_{0};
};
