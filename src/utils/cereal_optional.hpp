#pragma once

#include <experimental/optional>

namespace cereal {

template <class Archive, class T>
void save(Archive &ar, const std::experimental::optional<T> &opt) {
  ar(static_cast<bool>(opt));
  if (opt) {
    ar(*opt);
  }
}

template <class Archive, class T>
void load(Archive &ar, std::experimental::optional<T> &opt) {
  bool has_value;
  ar(has_value);
  if (has_value) {
    T tmp;
    ar(tmp);
    opt = std::move(tmp);
  } else {
    opt = std::experimental::nullopt;
  }
}

} // namespace cereal
