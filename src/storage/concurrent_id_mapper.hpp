#pragma once

/**
 * Defines an interface for mapping IDs to values and vice-versa. The interface
 * is necessary because in the distributed system implementations are different
 * for the master (single source of truth) and worker (must query master).
 * Both implementations must be concurrent.
 *
 * @TParam TId - ID type. Must expose `::TStorage`.
 * @TParam TRecord - Value type.
 */
template <typename TId, typename TValue>
class ConcurrentIdMapper {
 public:
  virtual TId value_to_id(const TValue &value) = 0;
  virtual const TValue &id_to_value(const TId &id) = 0;
};
