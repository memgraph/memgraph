#pragma once

#include <vector>
#include <map>

#include "typed_value.hpp"

/**
 * A collection of properties accessed in a map-like way
 * using a key of type Properties::TKey.
 *
 * The underlying implementation is not necessarily std::map.
 */
class TypedValueStore {
public:
  using sptr = std::shared_ptr<TypedValueStore>;

  /** The type of key used to get and set properties */
  using TKey = uint32_t;

  /**
   * Returns a TypedValue (by reference) at the given key.
   * If the key does not exist, the Null property is returned.
   *
   * This is NOT thread-safe, the reference might not be valid
   * when used in a multithreaded scenario.
   *
   * @param key The key for which a TypedValue is sought.
   * @return  See above.
   */
  const TypedValue &at(const TKey &key) const;

  /**
   * Sets the value for the given key. A new TypedValue instance
   * is created for the given value (which is of raw type).
   *
   * @tparam TValue Type of value. It must be one of the
   * predefined possible TypedValue values (bool, string, int...)
   * @param key  The key for which the property is set. The previous
   * value at the same key (if there was one) is replaced.
   * @param value  The value to set.
   */
  template<typename TValue>
  void set(const TKey &key, const TValue &value);

  /**
   * Set overriding for character constants. Forces conversion
   * to std::string, otherwise templating might cast the pointer
   * to something else (bool) and mess things up.
   *
   * @param key  The key for which the property is set. The previous
   * value at the same key (if there was one) is replaced.
   * @param value  The value to set.
   */
  void set(const TKey &key, const char *value);

  /**
   * Removes the TypedValue for the given key.
   *
   * @param key The key for which to remove the property.
   * @return  The number of removed properties (0 or 1).
   */
  size_t erase(const TKey &key);

  /**
   * @return The number of Properties in this collection.
   */
  size_t size() const;


  /**
   * Accepts two functions.
   *
   * @param handler  Called for each TypedValue in this collection.
   * @param finish Called once in the end.
   */
  void Accept(std::function<void(const TKey key, const TypedValue& prop)> handler,
              std::function<void()> finish = {}) const;

private:
  std::vector<std::pair<TKey, TypedValue>> props_;
};
