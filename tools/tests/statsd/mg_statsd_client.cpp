#include "gflags/gflags.h"

#include "stats/stats.hpp"
#include "utils/string.hpp"

// TODO (buda): move this logic to a unit test

// TODO (mtomic): This is a hack. I don't know a better way to make this work.
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"
BOOST_CLASS_EXPORT(stats::StatsReq);
BOOST_CLASS_EXPORT(stats::StatsRes);

bool parse_input(const std::string &s, std::string &metric_path,
                 std::vector<std::pair<std::string, std::string>> &tags,
                 double &value) {
  auto words = utils::Split(s, " ");
  if (words.size() < 2) {
    return false;
  }

  metric_path = words[0];

  try {
    value = std::stod(words.back());
  } catch (std::exception &e) {
    return false;
  }

  tags.clear();
  for (size_t i = 1; i < words.size() - 1; ++i) {
    auto tag_value = utils::Split(words[i], "=", 1);
    if (tag_value.size() != 2) {
      return false;
    }
    // TODO(mtomic): tags probably need to be escaped before sending to graphite
    tags.emplace_back(tag_value[0], tag_value[1]);
  }

  return true;
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(INFO) << "Usage: metric_path tag1=value1 ... tagn=valuen "
               "metric_value";

  InitStatsLogging();

  std::string line;
  std::string metric_path;
  std::vector<std::pair<std::string, std::string>> tags;
  double value;

  while (true) {
    std::getline(std::cin, line);
    if (!parse_input(line, metric_path, tags, value)) {
      LOG(ERROR) << "Invalid input";
      continue;
    }
    LogStat(metric_path, value, tags);
  }

  return 0;
}
