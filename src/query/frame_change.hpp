

#include <unordered_map>
#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

using CachedType = std::unordered_map<size_t, std::vector<TypedValue>>;

struct CachedValue {
  // Cached value, this can be probably templateized
  CachedType cache_;
  // Func to check if cache_ contains value
  std::function<bool(CachedValue &, const TypedValue &)> contains_value_;
  // Func to cache_value inside cache_
  std::function<bool(CachedValue &, const TypedValue &)> cache_value_;
};
class FrameChangeCollector {
 public:
  explicit FrameChangeCollector(utils::MemoryResource *mem) : tracked_values_(mem){};

  CachedValue &AddTrackingValue(const std::string &name) {
    tracked_values_.emplace(name, CachedValue{});
    return tracked_values_[name];
  }

  bool ContainsTrackingValue(const std::string &name) { return tracked_values_.contains(name); }

  bool IsTrackingValueCached(const std::string &name) {
    return tracked_values_.contains(name) && !tracked_values_[name].cache_.empty();
  }

  bool ResetTrackingValue(const std::string &name) {
    if (tracked_values_.contains(name)) {
      tracked_values_[name].cache_.clear();
    }

    return true;
  }

  CachedValue &GetCachedValue(const std::string &name) { return tracked_values_[name]; }

 private:
  memgraph::utils::pmr::unordered_map<std::string, CachedValue> tracked_values_;
};
}  // namespace memgraph::query
