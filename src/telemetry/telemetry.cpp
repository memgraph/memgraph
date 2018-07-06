#include "telemetry/telemetry.hpp"

#include <experimental/filesystem>

#include <fmt/format.h>
#include <glog/logging.h>

#include "requests/requests.hpp"
#include "telemetry/collectors.hpp"
#include "telemetry/system_info.hpp"
#include "utils/timestamp.hpp"
#include "utils/uuid.hpp"

namespace telemetry {

const int kMaxBatchSize = 100;

Telemetry::Telemetry(
    const std::string &url,
    const std::experimental::filesystem::path &storage_directory,
    std::chrono::duration<long long> refresh_interval,
    const uint64_t send_every_n)
    : url_(url),
      uuid_(utils::GenerateUUID()),
      send_every_n_(send_every_n),
      storage_(storage_directory) {
  StoreData("startup", GetSystemInfo());
  AddCollector("resources", GetResourceUsage);
  AddCollector("uptime", [&]() -> nlohmann::json { return GetUptime(); });
  scheduler_.Run("Telemetry", refresh_interval, [&] { CollectData(); });
}

void Telemetry::AddCollector(
    const std::string &name,
    const std::function<const nlohmann::json(void)> &func) {
  std::lock_guard<std::mutex> guard(lock_);
  collectors_.push_back({name, func});
}

Telemetry::~Telemetry() {
  scheduler_.Stop();
  CollectData("shutdown");
}

void Telemetry::StoreData(const nlohmann::json &event,
                          const nlohmann::json &data) {
  nlohmann::json payload = {
      {"id", uuid_},
      {"event", event},
      {"data", data},
      {"timestamp", utils::Timestamp::Now().SecWithNsecSinceTheEpoch()}};
  storage_.Put(fmt::format("{}:{}", uuid_, event.dump()), payload.dump());
}

void Telemetry::SendData() {
  std::vector<std::string> keys;
  nlohmann::json payload = nlohmann::json::array();

  int count = 0;
  for (auto it = storage_.begin();
       it != storage_.end() && count < kMaxBatchSize; ++it, ++count) {
    keys.push_back(it->first);
    try {
      payload.push_back(nlohmann::json::parse(it->second));
    } catch (const nlohmann::json::parse_error &e) {
      DLOG(WARNING) << "Couldn't convert " << it->second << " to json!";
    }
  }

  if (requests::RequestPostJson(url_, payload)) {
    for (const auto &key : keys) {
      if (!storage_.Delete(key)) {
        DLOG(WARNING) << "Couldn't delete key " << key
                      << " from telemetry storage!";
      }
    }
  }
}

void Telemetry::CollectData(const std::string &event) {
  nlohmann::json data = nlohmann::json::object();
  {
    std::lock_guard<std::mutex> guard(lock_);
    for (auto &collector : collectors_) {
      data[collector.first] = collector.second();
    }
  }
  if (event == "") {
    StoreData(num_++, data);
  } else {
    StoreData(event, data);
  }
  if (num_ % send_every_n_ == 0 || event == "shutdown") {
    SendData();
  }
}

const nlohmann::json Telemetry::GetUptime() { return timer_.Elapsed().count(); }

}  // namespace telemetry
