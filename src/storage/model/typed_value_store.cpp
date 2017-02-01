#include <algorithm>

#include "storage/model/typed_value_store.hpp"

const TypedValue& TypedValueStore::at(const TKey &key) const {
  for (const auto& kv : props_)
    if (kv.first == key)
      return kv.second;

  return TypedValue::Null;
}

size_t TypedValueStore::erase(const TKey &key) {
  auto found = std::find_if(props_.begin(), props_.end(), [&key](std::pair<TKey, TypedValue> &kv){return kv.first == key;});
  if (found != props_.end()) {
    props_.erase(found);
    return 1;
  }
  return 0;
}

size_t TypedValueStore::size() {
  return props_.size();
}

void TypedValueStore::Accept(std::function<void(const TypedValueStore::TKey, const TypedValue &)> handler,
                        std::function<void()> finish) const {
  if (handler)
    for (const auto& prop : props_)
      handler(prop.first, prop.second);

  if (finish)
    finish();

}
