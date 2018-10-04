#pragma once

#include <mutex>
#include <string>

#include <json/json.hpp>

#include "storage/kvstore/kvstore.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"

namespace telemetry {

/**
 * This class implements the telemetry collector service. It periodically scapes
 * all registered collectors and stores their data. With periodically scraping
 * the collectors the service collects machine information in the constructor
 * and stores it. Also, it calles all collectors once more in the destructor so
 * that final stats can be collected. All data is stored persistently. If there
 * is no internet connection the data will be sent when the internet connection
 * is reestablished. If there is an issue with the internet connection that
 * won't effect normal operation of the service (it won't crash).
 */
class Telemetry final {
 public:
  Telemetry(const std::string &url,
            const std::experimental::filesystem::path &storage_directory,
            std::chrono::duration<long long> refresh_interval =
                std::chrono::minutes(10),
            const uint64_t send_every_n = 10);

  void AddCollector(const std::string &name,
                    const std::function<const nlohmann::json(void)> &func);

  ~Telemetry();

  Telemetry(const Telemetry &) = delete;
  Telemetry(Telemetry &&) = delete;
  Telemetry &operator=(const Telemetry &) = delete;
  Telemetry &operator=(Telemetry &&) = delete;

 private:
  void StoreData(const nlohmann::json &event, const nlohmann::json &data);
  void SendData();
  void CollectData(const std::string &event = "");

  const nlohmann::json GetUptime();

  const std::string url_;
  const std::string uuid_;
  uint64_t num_{0};
  utils::Scheduler scheduler_;
  utils::Timer timer_;

  const uint64_t send_every_n_;

  std::mutex lock_;
  std::vector<std::pair<std::string, std::function<const nlohmann::json(void)>>>
      collectors_;

  storage::KVStore storage_;
};

}  // namespace telemetry
