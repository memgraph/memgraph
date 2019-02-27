#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <fstream>
#include <functional>
#include <limits>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "json/json.hpp"

#include "database/single_node/graph_db.hpp"
#include "integrations/kafka/streams.hpp"
#include "memgraph_init.hpp"
#include "utils/flag_validation.hpp"
#include "utils/timer.hpp"

using namespace std::literals::chrono_literals;

DEFINE_int64(import_count, 0, "How many entries should we import.");
DEFINE_int64(timeout, 60, "How many seconds should the benchmark wait.");
DEFINE_string(kafka_uri, "", "Kafka URI.");
DEFINE_string(topic_name, "", "Kafka topic.");
DEFINE_string(transform_uri, "", "Transform script URI.");
DEFINE_string(output_file, "", "Output file where shold the results be.");

void KafkaBenchmarkMain() {
  google::SetUsageMessage("Memgraph kafka benchmark database server");

  auto durability_directory =
      std::experimental::filesystem::path(FLAGS_durability_directory);

  auth::Init();
  auth::Auth auth{durability_directory / "auth"};

  audit::Log audit_log{durability_directory / "audit",
                       audit::kBufferSizeDefault,
                       audit::kBufferFlushIntervalMillisDefault};

  query::Interpreter interpreter;
  database::GraphDb db;
  SessionData session_data{&db, &interpreter, &auth, &audit_log};

  std::atomic<int64_t> query_counter{0};
  std::atomic<bool> timeout_reached{false};
  std::atomic<bool> benchmark_finished{false};

  integrations::kafka::Streams kafka_streams{
      std::experimental::filesystem::path(FLAGS_durability_directory) /
          "streams",
      [&session_data, &query_counter](
          const std::string &query,
          const std::map<std::string, communication::bolt::Value> &params) {
        KafkaStreamWriter(session_data, query, params);
        query_counter++;
      }};

  session_data.interpreter->auth_ = &auth;
  session_data.interpreter->kafka_streams_ = &kafka_streams;

  std::string stream_name = "benchmark";

  integrations::kafka::StreamInfo stream_info;
  stream_info.stream_name = stream_name;
  stream_info.stream_uri = FLAGS_kafka_uri;
  stream_info.stream_topic = FLAGS_topic_name;
  stream_info.transform_uri = FLAGS_transform_uri;

  kafka_streams.Create(stream_info);
  kafka_streams.StartAll();

  // Kickoff a thread that will timeout after FLAGS_timeout seconds
  std::thread timeout_thread_ =
      std::thread([&timeout_reached, &benchmark_finished]() {
        utils::ThreadSetName("BenchTimeout");
        for (int64_t i = 0; i < FLAGS_timeout; ++i) {
          std::this_thread::sleep_for(1s);
          if (benchmark_finished.load()) return;
        }

        timeout_reached.store(true);
      });

  // Wait for the import to start
  while (!timeout_reached.load() && query_counter.load() == 0) {
    std::this_thread::sleep_for(1ms);
  }

  int64_t query_count_start = query_counter.load();
  utils::Timer timer;

  // Wait for the import to finish
  while (!timeout_reached.load() && query_counter.load() < FLAGS_import_count) {
    std::this_thread::sleep_for(1ms);
  }

  double duration = timer.Elapsed().count();
  kafka_streams.StopAll();
  kafka_streams.Drop(stream_name);

  benchmark_finished.store(true);
  if (timeout_thread_.joinable()) timeout_thread_.join();

  int64_t writes = query_counter.load() - query_count_start;
  double write_per_second = writes / duration;

  std::ofstream output(FLAGS_output_file);
  output << "duration " << duration << std::endl;
  output << "executed_writes " << writes << std::endl;
  output << "write_per_second " << write_per_second << std::endl;
  output.close();
}

int main(int argc, char **argv) {
  return WithInit(argc, argv, KafkaBenchmarkMain);
}
