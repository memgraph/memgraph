#include "stats/metrics.hpp"

#include <tuple>

#include "fmt/format.h"
#include "glog/logging.h"

namespace stats {

std::mutex &MetricsMutex() {
  static std::mutex mutex;
  return mutex;
}

std::map<std::string, std::unique_ptr<Metric>> &AccessMetrics() {
  static std::map<std::string, std::unique_ptr<Metric>> metrics;
  MetricsMutex().lock();
  return metrics;
}

void ReleaseMetrics() { MetricsMutex().unlock(); }

Metric::Metric(int64_t start_value) : value_(start_value) {}

Counter::Counter(int64_t start_value) : Metric(start_value) {}

void Counter::Bump(int64_t delta) { value_ += delta; }

std::optional<int64_t> Counter::Flush() { return value_; }

int64_t Counter::Value() { return value_; }

Gauge::Gauge(int64_t start_value) : Metric(start_value) {}

void Gauge::Set(int64_t value) { value_ = value; }

std::optional<int64_t> Gauge::Flush() { return value_; }

IntervalMin::IntervalMin(int64_t start_value) : Metric(start_value) {}

void IntervalMin::Add(int64_t value) {
  int64_t curr = value_;
  while (curr > value && !value_.compare_exchange_weak(curr, value))
    ;
}

std::optional<int64_t> IntervalMin::Flush() {
  int64_t curr = value_;
  value_.compare_exchange_weak(curr, std::numeric_limits<int64_t>::max());
  return curr == std::numeric_limits<int64_t>::max() ? std::nullopt
                                                     : std::make_optional(curr);
}

IntervalMax::IntervalMax(int64_t start_value) : Metric(start_value) {}

void IntervalMax::Add(int64_t value) {
  int64_t curr = value_;
  while (curr < value && !value_.compare_exchange_weak(curr, value))
    ;
}

std::optional<int64_t> IntervalMax::Flush() {
  int64_t curr = value_;
  value_.compare_exchange_weak(curr, std::numeric_limits<int64_t>::min());
  return curr == std::numeric_limits<int64_t>::min() ? std::nullopt
                                                     : std::make_optional(curr);
}

template <class T>
T &GetMetric(const std::string &name, int64_t start_value) {
  auto &metrics = AccessMetrics();
  auto it = metrics.find(name);
  if (it == metrics.end()) {
    auto got = metrics.emplace(name, std::make_unique<T>(start_value));
    CHECK(got.second) << "Failed to create counter " << name;
    it = got.first;
  }
  ReleaseMetrics();
  auto *ptr = dynamic_cast<T *>(it->second.get());
  if (!ptr) {
    LOG(FATAL) << fmt::format("GetMetric({}) called with invalid metric type",
                              name);
  }
  return *ptr;
}

Counter &GetCounter(const std::string &name, int64_t start_value) {
  return GetMetric<Counter>(name, start_value);
}

Gauge &GetGauge(const std::string &name, int64_t start_value) {
  return GetMetric<Gauge>(name, start_value);
}

IntervalMin &GetIntervalMin(const std::string &name) {
  return GetMetric<IntervalMin>(name, std::numeric_limits<int64_t>::max());
}

IntervalMax &GetIntervalMax(const std::string &name) {
  return GetMetric<IntervalMax>(name, std::numeric_limits<int64_t>::min());
}

}  // namespace stats
