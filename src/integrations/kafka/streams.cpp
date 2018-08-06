#include "integrations/kafka/streams.hpp"

#include <cstdio>
#include <experimental/filesystem>
#include <experimental/optional>

#include <json/json.hpp>

#include "integrations/kafka/exceptions.hpp"
#include "requests/requests.hpp"
#include "utils/file.hpp"

namespace integrations::kafka {

namespace fs = std::experimental::filesystem;

const std::string kMetadataDir = "metadata";
const std::string kTransformDir = "transform";
const std::string kTransformExt = ".py";

namespace {

nlohmann::json Serialize(const StreamInfo &info) {
  nlohmann::json data = nlohmann::json::object();
  data["stream_name"] = info.stream_name;
  data["stream_uri"] = info.stream_uri;
  data["stream_topic"] = info.stream_topic;
  data["transform_uri"] = info.transform_uri;

  if (info.batch_interval_in_ms) {
    data["batch_interval_in_ms"] = info.batch_interval_in_ms.value();
  } else {
    data["batch_interval_in_ms"] = nullptr;
  }

  if (info.batch_size) {
    data["batch_size"] = info.batch_size.value();
  } else {
    data["batch_size"] = nullptr;
  }

  if (info.limit_batches) {
    data["limit_batches"] = info.limit_batches.value();
  } else {
    data["limit_batches"] = nullptr;
  }

  data["is_running"] = info.is_running;

  return data;
}

StreamInfo Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) throw StreamDeserializationException();

  StreamInfo info;

  if (!data["stream_name"].is_string()) throw StreamDeserializationException();
  info.stream_name = data["stream_name"];

  if (!data["stream_uri"].is_string()) throw StreamDeserializationException();
  info.stream_uri = data["stream_uri"];

  if (!data["stream_topic"].is_string()) throw StreamDeserializationException();
  info.stream_topic = data["stream_topic"];

  if (!data["transform_uri"].is_string())
    throw StreamDeserializationException();
  info.transform_uri = data["transform_uri"];

  if (data["batch_interval_in_ms"].is_number()) {
    info.batch_interval_in_ms = data["batch_interval_in_ms"];
  } else if (data["batch_interval_in_ms"].is_null()) {
    info.batch_interval_in_ms = std::experimental::nullopt;
  } else {
    throw StreamDeserializationException();
  }

  if (data["batch_size"].is_number()) {
    info.batch_size = data["batch_size"];
  } else if (data["batch_size"].is_null()) {
    info.batch_size = std::experimental::nullopt;
  } else {
    throw StreamDeserializationException();
  }

  if (!data["is_running"].is_boolean()) throw StreamDeserializationException();
  info.is_running = data["is_running"];

  if (data["limit_batches"].is_number()) {
    info.limit_batches = data["limit_batches"];
  } else if (data["limit_batches"].is_null()) {
    info.limit_batches = std::experimental::nullopt;
  } else {
    throw StreamDeserializationException();
  }

  return info;
}

}  // namespace

Streams::Streams(const std::string &streams_directory,
                 std::function<void(
                     const std::string &,
                     const std::map<std::string, communication::bolt::Value> &)>
                     stream_writer)
    : streams_directory_(streams_directory),
      stream_writer_(stream_writer),
      metadata_store_(fs::path(streams_directory) / kMetadataDir) {}

void Streams::Recover() {
  for (auto it = metadata_store_.begin(); it != metadata_store_.end(); ++it) {
    // Check if the transform script also exists;
    auto transform_script = GetTransformScriptPath(it->first);
    if (!fs::exists(transform_script))
      throw TransformScriptNotFoundException(it->first);

    nlohmann::json data;
    try {
      data = nlohmann::json::parse(it->second);
    } catch (const nlohmann::json::parse_error &e) {
      throw StreamDeserializationException();
    }

    StreamInfo info = Deserialize(data);
    Create(info, false);
    if (info.is_running) Start(info.stream_name, info.limit_batches);
  }
}

void Streams::Create(const StreamInfo &info, bool download_transform_script) {
  std::lock_guard<std::mutex> g(mutex_);
  if (consumers_.find(info.stream_name) != consumers_.end())
    throw StreamExistsException(info.stream_name);

  // Store stream_info in metadata_store_.
  if (!metadata_store_.Put(info.stream_name, Serialize(info).dump())) {
    throw StreamMetadataCouldNotBeStored(info.stream_name);
  }

  // Make sure transform directory exists or we can create it.
  if (!utils::EnsureDir(GetTransformScriptDir())) {
    throw TransformScriptCouldNotBeCreatedException(info.stream_name);
  }

  // Download the transform script.
  auto transform_script_path = GetTransformScriptPath(info.stream_name);
  if (download_transform_script &&
      !requests::CreateAndDownloadFile(info.transform_uri,
                                       transform_script_path)) {
    throw TransformScriptDownloadException(info.transform_uri);
  }

  try {
    consumers_.emplace(
        std::piecewise_construct, std::forward_as_tuple(info.stream_name),
        std::forward_as_tuple(info, transform_script_path, stream_writer_));
  } catch (const KafkaStreamException &e) {
    // If we failed to create the consumer, remove the persisted metadata.
    metadata_store_.Delete(info.stream_name);
    // Rethrow the exception.
    throw;
  }
}

void Streams::Drop(const std::string &stream_name) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  // Erase and implicitly stop the consumer.
  consumers_.erase(find_it);

  // Remove stream_info in metadata_store_.
  if (!metadata_store_.Delete(stream_name)) {
    throw StreamMetadataCouldNotBeDeleted(stream_name);
  }

  // Remove transform script.
  if (std::remove(GetTransformScriptPath(stream_name).c_str())) {
    throw TransformScriptNotFoundException(stream_name);
  }
}

void Streams::Start(const std::string &stream_name,
                    std::experimental::optional<int64_t> limit_batches) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  find_it->second.Start(limit_batches);

  // Store stream_info in metadata_store_.
  if (!metadata_store_.Put(stream_name,
                           Serialize(find_it->second.info()).dump())) {
    throw StreamMetadataCouldNotBeStored(stream_name);
  }
}

void Streams::Stop(const std::string &stream_name) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  find_it->second.Stop();

  // Store stream_info in metadata_store_.
  if (!metadata_store_.Put(stream_name,
                           Serialize(find_it->second.info()).dump())) {
    throw StreamMetadataCouldNotBeStored(stream_name);
  }
}

void Streams::StartAll() {
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    consumer_kv.second.StartIfStopped();

    // Store stream_info in metadata_store_.
    if (!metadata_store_.Put(consumer_kv.first,
                             Serialize(consumer_kv.second.info()).dump())) {
      throw StreamMetadataCouldNotBeStored(consumer_kv.first);
    }
  }
}

void Streams::StopAll() {
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    consumer_kv.second.StopIfRunning();

    // Store stream_info in metadata_store_.
    if (!metadata_store_.Put(consumer_kv.first,
                             Serialize(consumer_kv.second.info()).dump())) {
      throw StreamMetadataCouldNotBeStored(consumer_kv.first);
    }
  }
}

std::vector<StreamStatus> Streams::Show() {
  std::vector<StreamStatus> streams;
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    streams.emplace_back(consumer_kv.second.Status());
  }

  return streams;
}

std::vector<
    std::pair<std::string, std::map<std::string, communication::bolt::Value>>>
Streams::Test(const std::string &stream_name,
              std::experimental::optional<int64_t> limit_batches) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  return find_it->second.Test(limit_batches);
}

std::string Streams::GetTransformScriptDir() {
  return fs::path(streams_directory_) / kTransformDir;
}

std::string Streams::GetTransformScriptPath(const std::string &stream_name) {
  return fs::path(GetTransformScriptDir()) / (stream_name + kTransformExt);
}

}  // namespace integrations::kafka
