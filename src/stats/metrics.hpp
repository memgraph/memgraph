/**
 * @file
 *
 * This file contains some metrics types that can be aggregated on client side
 * and periodically flushed to StatsD.
 */
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "fmt/format.h"

namespace stats {

// TODO(mtomic): it would probably be nice to have Value method for every metric
// type, however, there is no use case for this yet

/**
 * Abstract base class for all metrics.
 */
class Metric {
 public:
  /**
   * Constructs a metric to be exported to StatsD.
   *
   * @param name  metric will be exported to StatsD with this path
   * @param value initial value
   */
  virtual ~Metric() {}

  /**
   * Metric refresh thread will periodically call this function. It should
   * return the metric value aggregated since the last flush call or nullopt
   * if there were no updates.
   */
  virtual std::optional<int64_t> Flush() = 0;

  explicit Metric(int64_t start_value = 0);

 protected:
  std::atomic<int64_t> value_;
};

/**
 * A simple counter.
 */
class Counter : public Metric {
 public:
  explicit Counter(int64_t start_value = 0);

  /**
   * Change counter value by delta.
   *
   * @param delta value change
   */
  void Bump(int64_t delta = 1);

  /** Returns the current value of the counter. **/
  std::optional<int64_t> Flush() override;

  /** Returns the current value of the counter. **/
  int64_t Value();

  friend Counter &GetCounter(const std::string &name);
};

/**
 * To be used instead of Counter constructor. If counter with this name doesn't
 * exist, it will be initialized with start_value.
 *
 * @param name        counter name
 * @param start_value start value
 */
Counter &GetCounter(const std::string &name, int64_t start_value = 0);

/**
 * A simple gauge. Gauge value is explicitly set, instead of being added to or
 * subtracted from.
 */
class Gauge : public Metric {
 public:
  explicit Gauge(int64_t start_value = 0);

  /**
   * Set gauge value.
   *
   * @param value value to be set
   */
  void Set(int64_t value);

  /** Returns the current gauge value. **/
  std::optional<int64_t> Flush() override;
};

/**
 * To be used instead of Gauge constructor. If gauge with this name doesn't
 * exist, it will be initialized with start_value.
 *
 * @param name        gauge name
 * @param start_value start value
 */
Gauge &GetGauge(const std::string &name, int64_t start_value = 0);

/**
 * Aggregates minimum between two flush periods.
 */
class IntervalMin : public Metric {
 public:
  explicit IntervalMin(int64_t start_value);

  /**
   * Add another value into the minimum computation.
   *
   * @param value value to be added
   */
  void Add(int64_t value);

  /**
   * Returns the minimum value encountered since the last flush period,
   * or nullopt if no values were added.
   */
  std::optional<int64_t> Flush() override;
};

/**
 * To be used instead of IntervalMin constructor.
 *
 * @param name        interval min name
 */
IntervalMin &GetIntervalMin(const std::string &name);

/**
 * Aggregates maximum betweenw two flush periods.
 */
class IntervalMax : public Metric {
 public:
  explicit IntervalMax(int64_t start_value);

  /**
   * Add another value into the maximum computation.
   */
  void Add(int64_t value);

  /**
   * Returns the maximum value encountered since the last flush period,
   * or nullopt if no values were added.
   */
  std::optional<int64_t> Flush() override;
};

/**
 * To be used instead of IntervalMax constructor.
 *
 * @param name        interval max name
 */
IntervalMax &GetIntervalMax(const std::string &name);

/**
 * A stopwatch utility. It exports 4 metrics: total time measured since the
 * beginning of the program, total number of times time intervals measured,
 * minimum and maximum time interval measured since the last metric flush.
 * Metrics exported by the stopwatch will be named
 * [name].{total_time|count|min|max}.
 *
 * @param name timed event name
 * @param f Callable, an action to be performed.
 */
template <class Function>
int64_t Stopwatch(const std::string &name, Function f) {
  auto &total_time = GetCounter(fmt::format("{}.total_time", name));
  auto &count = GetCounter(fmt::format("{}.count", name));
  auto &min = GetIntervalMin(fmt::format("{}.min", name));
  auto &max = GetIntervalMax(fmt::format("{}.max", name));
  auto start = std::chrono::system_clock::now();
  f();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now() - start)
                      .count();
  total_time.Bump(duration);
  count.Bump();
  min.Add(duration);
  max.Add(duration);
  return duration;
}

/**
 * Access internal metric list. You probably don't want to use this,
 * but if you do, make sure to call ReleaseMetrics when you're done.
 */
std::map<std::string, std::unique_ptr<Metric>> &AccessMetrics();

/**
 * Releases internal lock on metric list.
 */
void ReleaseMetrics();

}  // namespace stats
