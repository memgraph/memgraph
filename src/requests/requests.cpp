// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "requests/requests.hpp"

#include <cstdio>

#include <curl/curl.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <ctre.hpp>

#include "utils/counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

namespace memgraph::requests {

namespace {

struct ProgressData {
  std::function<void()> abort_check_;
  std::optional<std::chrono::steady_clock::time_point> last_tp_;
};

// Callback function for reporting progress during a file download
auto DownloadProgressCb(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t /*ultotal*/,
                        curl_off_t /*ulnow*/) -> int {
  // No need to update the progress
  if (dltotal == 0) return 0;

  constexpr auto kAbortTransferReturnCode = 1;
  constexpr auto kContinueTransferReturnCode = 0;

  auto *data = static_cast<ProgressData *>(clientp);

  // Catch HintedAbortError and abort the transfer if got the request to terminate the transactions
  // abort_check_ could be a nullptr
  if (data->abort_check_) {
    try {
      data->abort_check_();
    } catch (std::exception const &e) {
      return kAbortTransferReturnCode;
    }
  }

  static auto counter = utils::ResettableCounter(500);

  // Don't log too often but log when the file download is complete
  if (counter() || dlnow == dltotal) {
    auto const progress = (100.0F * static_cast<float>(dlnow)) / static_cast<float>(dltotal);
    spdlog::trace("Downloaded {:.2f}% of the file", progress);
  }

  auto const now = std::chrono::steady_clock::now();

  // If not the first call, check whether it passed more than 10s between callbacks
  if (LIKELY(data->last_tp_.has_value())) {
    constexpr auto download_timeout = 10;
    // Steady clock guarantees this won't underflow
    if (now - *(data->last_tp_) > std::chrono::seconds{download_timeout}) {
      // Signal to the libcurl that it should abort the transfer
      return kAbortTransferReturnCode;
    }
  }

  data->last_tp_.emplace(now);

  return kContinueTransferReturnCode;
}

size_t CurlWriteCallback(char * /*ptr*/, size_t /*size*/, size_t nmemb, void * /*userdata*/) { return nmemb; }

}  // namespace

void Init() { curl_global_init(CURL_GLOBAL_ALL); }

bool RequestPostJson(const std::string &url, const nlohmann::json &data, int timeout_in_seconds) {
  CURL *curl = nullptr;
  CURLcode res = CURLE_UNSUPPORTED_PROTOCOL;

  auto response_code = 0;
  struct curl_slist *headers = nullptr;
  std::string payload = data.dump();
  std::string user_agent = fmt::format("memgraph/{}", gflags::VersionString());

  curl = curl_easy_init();
  if (!curl) return false;

  headers = curl_slist_append(headers, "Accept: application/json");
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "charsets: utf-8");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
  curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_in_seconds);

  res = curl_easy_perform(curl);
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    SPDLOG_WARN("Couldn't perform request: {}", curl_easy_strerror(res));
    return false;
  }

  if (response_code != 200) {
    SPDLOG_WARN("Request response code isn't 200 (received {})!", response_code);
    return false;
  }

  return true;
}

bool CreateAndDownloadFile(const std::string &url, const std::string &path, uint64_t const connection_timeout,
                           std::function<void()> abort_check) {
  CURL *curl = nullptr;
  CURLcode res = CURLE_UNSUPPORTED_PROTOCOL;

  auto const user_agent = fmt::format("memgraph/{}", gflags::VersionString());

  curl = curl_easy_init();
  if (!curl) {
    spdlog::error("requests: Couldn't init curl");
    return false;
  }

  FILE *file = fopen(path.c_str(), "wb");
  if (!file) {
    spdlog::error("requests: Couldn't open file {} for writing", path);
    return false;
  }

  ProgressData progress_data{.abort_check_ = std::move(abort_check)};

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);
  // Timeout for establishing a connection
  // Includes DNS, all protocol handshakes and negotiations until there is an established connection with the remote
  // side
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connection_timeout);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
  curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.c_str());
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10);
  // Needed so that XFERINFOFUNCTION could work
  curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);
  curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progress_data);
  curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, DownloadProgressCb);

  res = curl_easy_perform(curl);

  if (std::fclose(file) != 0) {
    spdlog::error("Couldn't successfully close the file while downloading file {} {}", url, path);
    return false;
  }

  if (res != CURLE_OK) {
    spdlog::error("Error happened while downloading file {}: {}", url, curl_easy_strerror(res));
    return false;
  }

  curl_easy_cleanup(curl);

  return true;
}

auto DownloadToStream(char const *url, std::ostream &os) -> bool {
  constexpr auto WriteCallback = [](char *ptr, size_t size, size_t nmemb, std::ostream *os) -> size_t {
    auto const totalSize = static_cast<std::streamsize>(size * nmemb);
    os->write(ptr, totalSize);
    return totalSize;
  };

  auto *curl_handle{curl_easy_init()};
  curl_easy_setopt(curl_handle, CURLOPT_URL, url);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, +WriteCallback);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &os);

  auto const res = curl_easy_perform(curl_handle);
  long response_code = 0;  // NOLINT
  curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl_handle);

  if (res != CURLE_OK) {
    SPDLOG_WARN("Couldn't perform request: {}", curl_easy_strerror(res));
    return false;
  }

  constexpr auto protocol_matcher = ctre::starts_with<"(https?|ftp)://">;
  if (protocol_matcher(url) && response_code != 200) {
    SPDLOG_WARN("Request response code isn't 200 (received {})!", response_code);
    return false;
  }

  return true;
}

auto UrlToStringStream(const char *url) -> std::stringstream {
  auto ss = std::stringstream{};
  if (!requests::DownloadToStream(url, ss)) {
    throw utils::BasicException("CSV was unable to be fetched from {}", url);
  }
  return ss;
};

}  // namespace memgraph::requests
