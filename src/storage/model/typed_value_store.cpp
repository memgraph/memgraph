#include <algorithm>

#include "storage/model/typed_value_store.hpp"

const TypedValue& TypedValueStore::at(const TKey &key) const {
  for (const auto& kv : props_)
    if (kv.first == key)
      return kv.second;

  return TypedValue::Null;
}

template<typename TValue>
void TypedValueStore::set(const TKey &key, const TValue &value) {
  for (auto& kv: props_)
    if (kv.first == key) {
      kv.second = TypedValue(value);
      return;
    }

  // there is no value for the given key, add new
  // TODO consider vector size increment optimization
  props_.push_back(std::move(std::make_pair(key, value)));
}

// instantiations of the TypedValueStore::set function
// instances must be made for all of the supported C++ types
template void TypedValueStore::set<std::string>(const TKey &key, const std::string &value);
template void TypedValueStore::set<bool>(const TKey &key, const bool &value);
template void TypedValueStore::set<int>(const TKey &key, const int &value);
template void TypedValueStore::set<float>(const TKey &key, const float &value);

/**
 * Set overriding for character constants. Forces conversion
 * to std::string, otherwise templating might cast the pointer
 * to something else (bool) and mess things up.
 *
 * @param key  The key for which the property is set. The previous
 * value at the same key (if there was one) is replaced.
 * @param value  The value to set.
 */
void TypedValueStore::set(const TKey &key, const char *value) {
  set(key, std::string(value));
}

size_t TypedValueStore::erase(const TKey &key) {
  auto found = std::find_if(props_.begin(), props_.end(), [&key](std::pair<TKey, TypedValue> &kv){return kv.first == key;});
  if (found != props_.end()) {
    props_.erase(found);
    return 1;
  }
  return 0;
}

size_t TypedValueStore::size() const { return props_.size(); }

void TypedValueStore::Accept(std::function<void(const TypedValueStore::TKey, const TypedValue &)> handler,
                        std::function<void()> finish) const {
  if (handler)
    for (const auto& prop : props_)
      handler(prop.first, prop.second);

  if (finish)
    finish();

}


