#include "query/temporal.hpp"

namespace query {
namespace {
template <typename TFirst, typename TSecond>
auto GetAndSubtractDuration(TSecond &base_duration) {
  const auto duration = std::chrono::duration_cast<TFirst>(base_duration);
  base_duration -= duration;
  return duration.count();
}
}  // namespace

Date::Date(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
  months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
  days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
}

auto Microseconds() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::years{years} + std::chrono::months{months} +
                                                               std::chrono::days{days})
      .count();
}

}  // namespace query
