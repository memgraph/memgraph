#include "query/plan/profile.hpp"

#include <algorithm>
#include <chrono>

#include <fmt/format.h>

#include "query/context.hpp"
#include "utils/likely.hpp"

namespace query::plan {

//////////////////////////////////////////////////////////////////////////////
//
// FormatProfilingStats

namespace {

class FormatProfilingStatsHelper {
 public:
  FormatProfilingStatsHelper(unsigned long long total_cycles,
                             std::chrono::duration<double> total_time)
      : total_cycles_(total_cycles), total_time_(total_time) {}

  void Output(const ProfilingStats &cumulative_stats) {
    auto cycles = IndividualCycles(cumulative_stats);

    rows_.emplace_back(std::vector<TypedValue>{
        FormatOperator(cumulative_stats.name), cumulative_stats.actual_hits,
        FormatRelativeTime(cycles), FormatAbsoluteTime(cycles)});

    for (size_t i = 1; i < cumulative_stats.children.size(); ++i) {
      Branch(cumulative_stats.children[i]);
    }

    if (cumulative_stats.children.size() >= 1) {
      Output(cumulative_stats.children[0]);
    }
  }

  void Branch(const ProfilingStats &cumulative_stats) {
    rows_.emplace_back(std::vector<TypedValue>{"|\\", "", "", ""});

    ++depth_;
    Output(cumulative_stats);
    --depth_;
  }

  unsigned long long IndividualCycles(const ProfilingStats &cumulative_stats) {
    return cumulative_stats.num_cycles -
           std::accumulate(
               cumulative_stats.children.begin(),
               cumulative_stats.children.end(), 0ULL,
               [](auto acc, auto &stats) { return acc + stats.num_cycles; });
  }

  std::string Format(const char *str) {
    std::ostringstream ss;
    for (int i = 0; i < depth_; ++i) {
      ss << "| ";
    }
    ss << str;
    return ss.str();
  }

  std::string Format(const std::string &str) { return Format(str.c_str()); }

  std::string FormatOperator(const char *str) {
    return Format(std::string("* ") + str);
  }

  std::string FormatRelativeTime(unsigned long long num_cycles) {
    return fmt::format("{: 10.6f} %",
                       static_cast<double>(num_cycles) / total_cycles_ * 100);
  }

  std::string FormatAbsoluteTime(unsigned long long num_cycles) {
    return fmt::format(
        "{: 10.6f} ms",
        (static_cast<double>(num_cycles) / total_cycles_ *
         static_cast<std::chrono::duration<double, std::milli>>(total_time_))
            .count());
  }

  std::vector<std::vector<TypedValue>> rows() { return rows_; }

 private:
  int64_t depth_{0};
  std::vector<std::vector<TypedValue>> rows_;
  unsigned long long total_cycles_;
  std::chrono::duration<double> total_time_;
};

}  // namespace

std::vector<std::vector<TypedValue>> FormatProfilingStats(
    const ProfilingStats &cumulative_stats,
    std::chrono::duration<double> total_time) {
  FormatProfilingStatsHelper helper{cumulative_stats.num_cycles, total_time};
  helper.Output(cumulative_stats);
  return helper.rows();
}

}  // namespace query::plan
