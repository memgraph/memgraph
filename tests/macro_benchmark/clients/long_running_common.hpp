#pragma once

#include "json/json.hpp"

#include "bolt_client.hpp"
#include "common.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "stats/stats.hpp"
#include "utils/network.hpp"
#include "utils/timer.hpp"

const int MAX_RETRIES = 30;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(output, "", "Output file");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_int32(duration, 30, "Number of seconds to execute benchmark");

DEFINE_string(group, "unknown", "Test group name");
DEFINE_string(scenario, "unknown", "Test scenario name");

class TestClient {
 public:
  TestClient()
      : client_(FLAGS_address, FLAGS_port, FLAGS_username, FLAGS_password) {}

  virtual ~TestClient() {}

  auto ConsumeStats() {
    std::unique_lock<SpinLock> guard(lock_);
    auto stats = stats_;
    stats_.clear();
    return stats;
  }

  void Run() {
    runner_thread_ = std::thread([&] {
      while (keep_running_) {
        Step();
      }
    });
  }

  void Stop() {
    keep_running_ = false;
    runner_thread_.join();
  }

 protected:
  virtual void Step() = 0;

  communication::bolt::QueryData Execute(
      const std::string &query,
      const std::map<std::string, DecodedValue> &params,
      const std::string &query_name = "") {
    utils::Timer timer;
    auto result = ExecuteNTimesTillSuccess(client_, query, params, MAX_RETRIES);
    auto wall_time = timer.Elapsed();
    auto metadata = result.metadata;
    metadata["wall_time"] = wall_time.count();
    {
      std::unique_lock<SpinLock> guard(lock_);
      if (query_name != "") {
        stats_[query_name].push_back(std::move(metadata));
      } else {
        stats_[query].push_back(std::move(metadata));
      }
    }
    return result;
  }

  SpinLock lock_;
  std::unordered_map<std::string,
                     std::vector<std::map<std::string, DecodedValue>>>
      stats_;

  std::atomic<bool> keep_running_{true};
  std::thread runner_thread_;

 private:
  BoltClient client_;
};

void RunMultithreadedTest(std::vector<std::unique_ptr<TestClient>> &clients) {
  static const auto HOSTNAME =
      utils::GetHostname().value_or("unknown_hostname");
  static const auto TEST_PREFIX = fmt::format("{}.long_running.{}.{}", HOSTNAME,
                                              FLAGS_group, FLAGS_scenario);
  static const auto EXECUTED_QUERIES =
      fmt::format("{}.executed_queries", TEST_PREFIX);

  CHECK((int)clients.size() == FLAGS_num_workers);

  // Open stream for writing stats.
  std::streambuf *buf;
  std::ofstream f;
  if (FLAGS_output != "") {
    f.open(FLAGS_output);
    buf = f.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }
  std::ostream out(buf);

  utils::Timer timer;
  for (auto &client : clients) {
    client->Run();
  }
  LOG(INFO) << "Starting test with " << clients.size() << " workers";
  uint64_t executed_queries = 0;
  while (timer.Elapsed().count() < FLAGS_duration) {
    std::unordered_map<std::string, std::map<std::string, DecodedValue>>
        aggregated_stats;

    using namespace std::chrono_literals;
    std::unordered_map<std::string,
                       std::vector<std::map<std::string, DecodedValue>>>
        stats;
    for (const auto &client : clients) {
      auto client_stats = client->ConsumeStats();
      for (const auto &client_query_stats : client_stats) {
        auto &query_stats = stats[client_query_stats.first];
        query_stats.insert(query_stats.end(), client_query_stats.second.begin(),
                           client_query_stats.second.end());
        executed_queries +=
            client_query_stats.second.end() - client_query_stats.second.begin();
      }
    }

    // TODO: Here we combine pure values, json and DecodedValue which is a
    // little bit chaotic. Think about refactoring this part to only use json
    // and write DecodedValue to json converter.
    const std::vector<std::string> fields = {
        "wall_time", "parsing_time", "planning_time", "plan_execution_time",
    };
    for (const auto &query_stats : stats) {
      std::map<std::string, double> new_aggregated_query_stats;
      for (const auto &stats : query_stats.second) {
        for (const auto &field : fields) {
          auto it = stats.find(field);
          if (it != stats.end()) {
            new_aggregated_query_stats[field] += it->second.ValueDouble();
          }
        }
      }
      int64_t new_count = query_stats.second.size();

      auto &aggregated_query_stats = aggregated_stats[query_stats.first];
      aggregated_query_stats.insert({"count", DecodedValue(0)});
      auto old_count = aggregated_query_stats["count"].ValueInt();
      aggregated_query_stats["count"].ValueInt() += new_count;
      for (const auto &stat : new_aggregated_query_stats) {
        auto it = aggregated_query_stats.insert({stat.first, DecodedValue(0.0)})
                      .first;
        it->second =
            (it->second.ValueDouble() * old_count + stat.second * new_count) /
            (old_count + new_count);
      }
    }

    LogStat(EXECUTED_QUERIES, executed_queries);

    out << "{\"num_executed_queries\": " << executed_queries << ", "
        << "\"elapsed_time\": " << timer.Elapsed().count()
        << ", \"queries\": [";
    utils::PrintIterable(out, aggregated_stats, ", ", [](auto &stream,
                                                         const auto &x) {
      stream << "{\"query\": " << nlohmann::json(x.first) << ", \"stats\": ";
      PrintJsonDecodedValue(stream, DecodedValue(x.second));
      stream << "}";
    });
    out << "]}" << std::endl;
    out.flush();
    std::this_thread::sleep_for(1s);
  }
  LOG(INFO) << "Stopping workers...";
  for (auto &client : clients) {
    client->Stop();
  }
  LOG(INFO) << "Stopped workers...";
}
