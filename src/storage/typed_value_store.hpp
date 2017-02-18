#pragma once

#include <algorithm>
#include <map>
#include <vector>

#include "typed_value.hpp"

/**
 * A collection of properties accessed in a map-like way
 * using a key of type Properties::TKey.
 *
 * The underlying implementation is not necessarily std::map.
 *
  * @tparam TKey The type of key used in this value store.
 */
template <typename TKey = uint32_t>
class TypedValueStore {
 public:
  using sptr = std::shared_ptr<TypedValueStore>;

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
  const TypedValue &at(const TKey &key) const {
    for (const auto &kv : props_)
      if (kv.first == key) return kv.second;

    return TypedValue::Null;
  }

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
  template <typename TValue>
  void set(const TKey &key, const TValue &value) {
    for (auto &kv : props_)
      if (kv.first == key) {
        kv.second = TypedValue(value);
        return;
      }

    // there is no value for the given key, add new
    // TODO consider vector size increment optimization
    props_.push_back(std::move(std::make_pair(key, value)));
  }

  /**
   * Set overriding for character constants. Forces conversion
   * to std::string, otherwise templating might cast the pointer
   * to something else (bool) and mess things up.
   *
   * @param key  The key for which the property is set. The previous
   * value at the same key (if there was one) is replaced.
   * @param value  The value to set.
   */
  void set(const TKey &key, const char *value) { set(key, std::string(value)); }

  /**
   * Removes the TypedValue for the given key.
   *
   * @param key The key for which to remove the property.
   * @return  The number of removed properties (0 or 1).
   */
  size_t erase(const TKey &key) {
    auto found = std::find_if(
        props_.begin(), props_.end(),
        [&key](std::pair<TKey, TypedValue> &kv) { return kv.first == key; });

    if (found != props_.end()) {
      props_.erase(found);
      return 1;
    }
    return 0;
  }

  /**
   * @return The number of Properties in this collection.
   */
  size_t size() const { return props_.size(); }

  /**
   * Returns a const iterator over key-value pairs.
   *
   * @return See above.
   */
  auto begin() const { return props_.begin(); }

  /**
   * Returns an end iterator.
   *
   * @return See above.
   */
  auto end() const { return props_.end(); }

  /**
   * Accepts two functions.
   *
   * @param handler  Called for each TypedValue in this collection.
   * @param finish Called once in the end.
   */
  void Accept(std::function<void(const TKey, const TypedValue &)> handler,
              std::function<void()> finish = {}) const {
    if (handler)
      for (const auto &prop : props_) handler(prop.first, prop.second);

    if (finish) finish();
  }

 private:
  std::vector<std::pair<TKey, TypedValue>> props_;
};
