#pragma once

#include <experimental/optional>
#include <random>
#include <unordered_map>

#include "glog/logging.h"
#include "json/json.hpp"

#include "storage/property_value.hpp"

namespace snapshot_generation {

// Helper class for property value generation.
class ValueGenerator {
  using json = nlohmann::json;

 public:
  // Generates the whole property map based on the given config.
  std::unordered_map<std::string, PropertyValue> MakeProperties(
      const json &config) {
    std::unordered_map<std::string, PropertyValue> props;
    if (config.is_null()) return props;

    CHECK(config.is_object()) << "Properties config must be a dict";
    for (auto it = config.begin(); it != config.end(); it++) {
      auto value = MakeValue(it.value());
      if (value) props.emplace(it.key(), *value);
    }
    return props;
  }

  // Generates a single value based on the given config.
  std::experimental::optional<PropertyValue> MakeValue(const json &config) {
    if (config.is_object()) {
      const std::string &type = config["type"];
      const auto &param = config["param"];
      if (type == "primitive") return Primitive(param);
      if (type == "counter") return Counter(param);
      if (type == "optional") return Optional(param);
      if (type == "bernoulli") return Bernoulli(param);
      if (type == "randint") return RandInt(param);
      if (type == "randstring") return RandString(param);

      LOG(FATAL) << "Unknown value type";
    } else {
      return Primitive(config);
    }
  }

  // Returns whatever value is stored in the JSON
  PropertyValue Primitive(const json &config) {
    if (config.is_string()) return config.get<std::string>();
    if (config.is_number_integer()) return config.get<int64_t>();
    if (config.is_number_float()) return config.get<double>();
    if (config.is_boolean()) return config.get<bool>();

    LOG(FATAL) << "Unsupported primitive type";
  }

  // Increments the counter and returns its current value
  int64_t Counter(const std::string &name) { return counters_[name]++; }

  // Generates a random integer in range specified by JSON config
  int64_t RandInt(const json &range) {
    CHECK(range.is_array() && range.size() == 2)
        << "RandInt value gen config must be a list with 2 elements";
    auto from = MakeValue(range[0])->Value<int64_t>();
    auto to = MakeValue(range[1])->Value<int64_t>();
    CHECK(from < to) << "RandInt lower range must be lesser than upper range";
    return (int64_t)(rand_(gen_) * (to - from)) + from;
  }

  // Generates a random alphanumeric string with length specified by JSON config
  std::string RandString(const json &length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    int length_int = MakeValue(length)->Value<int64_t>();
    std::string r_val(length_int, 'a');
    for (int i = 0; i < length_int; ++i)
      r_val[i] = alphanum[(int64_t)(rand_(gen_) * strlen(alphanum))];

    return r_val;
  }

  // Returns true with given probability
  bool Bernoulli(double p) { return rand_(gen_) < p; }

  // Returns a value specified by config with some probability, and nullopt
  // otherwise
  std::experimental::optional<PropertyValue> Optional(const json &config) {
    CHECK(config.is_array() && config.size() == 2)
        << "Optional value gen config must be a list with 2 elements";
    return Bernoulli(config[0]) ? MakeValue(config[1])
                                : std::experimental::nullopt;
  }

 private:
  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};
  std::unordered_map<std::string, int64_t> counters_;
};

}  // namespace snapshot_generation
