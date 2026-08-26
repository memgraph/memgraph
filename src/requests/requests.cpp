// Copyright 2026 Memgraph Ltd.
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

#include <curl/curl.h>
#include <curl/system.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <array>
#include <chrono>
#include <compare>
#include <cstdio>
#include <ctre.hpp>
#include <exception>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <optional>
#include <sstream>
#include <utility>

#include "flags/general.hpp"
#include "spdlog/spdlog.h"
#include "utils/counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"
#include "utils/on_scope_exit.hpp"

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
  // abort_check_ could be a nullptr.
  if (data->abort_check_) {
    try {
      data->abort_check_();
    } catch (std::exception const &e) {
      return kAbortTransferReturnCode;
    }
  }

  static thread_local auto counter = utils::ResettableCounter(500);

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

// Progress callback for POST requests - checks per-request abort flag
auto PostProgressCallback(void *clientp, curl_off_t /*dltotal*/, curl_off_t /*dlnow*/, curl_off_t /*ultotal*/,
                          curl_off_t /*ulnow*/) -> int {
  auto const *abort_flag = static_cast<std::atomic<bool> const *>(clientp);
  if (abort_flag && abort_flag->load(std::memory_order_relaxed)) {
    return 1;  // Return non-zero to abort transfer
  }
  return 0;  // Continue transfer
}

// libcurl's default CA bundle path is a build-time constant, wrong on distros
// other than the build host's; use --ca-bundle-file if set, otherwise probe the
// standard locations. Resolved once and cached for the process lifetime.
const char *ResolveCaBundle() {
  static const std::string resolved = []() -> std::string {
    // An explicit path is passed through unvalidated so a bad value fails
    // loudly (CURLE_SSL_CACERT_BADFILE) instead of silently falling back.
    if (!FLAGS_ca_bundle_file.empty()) return FLAGS_ca_bundle_file;
    constexpr std::array paths = {
        "/etc/ssl/certs/ca-certificates.crt",      // Debian/Ubuntu
        "/etc/pki/tls/certs/ca-bundle.crt",        // RHEL/Fedora/CentOS/Rocky
        "/etc/ssl/ca-bundle.pem",                  // SUSE
        "/usr/local/share/certs/ca-root-nss.crt",  // FreeBSD (ca_root_nss port)
        "/etc/ssl/cert.pem",                       // OpenBSD, Alpine and others
    };
    for (const auto *path : paths) {
      std::error_code ec;
      if (std::filesystem::is_regular_file(path, ec)) return path;
    }
    spdlog::warn(
        "No system CA bundle found in the standard locations; https requests may fail certificate verification. Set "
        "--ca-bundle-file to fix this.");
    return "";  // keep libcurl's compiled-in default
  }();
  return resolved.empty() ? nullptr : resolved.c_str();
}

void SetCaInfo(CURL *curl) {
  if (const char *bundle = ResolveCaBundle(); bundle != nullptr) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, bundle);
  }
}

}  // namespace

void Init() { curl_global_init(CURL_GLOBAL_ALL); }

bool RequestPostJson(const std::string &url, const nlohmann::json &data, int timeout_in_seconds,
                     std::atomic<bool> const *abort_flag) {
  CURL *curl = nullptr;
  CURLcode res = CURLE_UNSUPPORTED_PROTOCOL;

  long response_code = 0;  // NOLINT(google-runtime-int) - curl_easy_getinfo requires long*
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
  SetCaInfo(curl);

  // Enable progress callback so an in-flight transfer can be aborted
  curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
  curl_easy_setopt(curl, CURLOPT_XFERINFODATA, abort_flag);
  curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, PostProgressCallback);

  res = curl_easy_perform(curl);
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl);
  curl_slist_free_all(headers);

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

// File will be destroyed when it goes out of scope by calling std::fclose.
// Clients are responsible for deleting the file if the downnload fails
bool CreateAndDownloadFile(const std::string &url, utils::FileUniquePtr file, uint64_t const connection_timeout,
                           std::function<void()> abort_check) {
  // A short write ends the transfer, which is what a full disk should do.
  auto const sink = [&file](char const *data, size_t const size) -> size_t {
    return std::fwrite(data, 1, size, file.get());
  };

  return DownloadToSink(url, sink, connection_timeout, std::move(abort_check));
}

bool DownloadToSink(const std::string &url, WriteSink const &write, uint64_t const connection_timeout,
                    std::function<void()> abort_check) {
  auto const user_agent = fmt::format("memgraph/{}", gflags::VersionString());

  auto *curl = curl_easy_init();
  if (!curl) {
    spdlog::error("requests: Couldn't init curl");
    return false;
  }
  utils::OnScopeExit const cleanup{[curl]() { curl_easy_cleanup(curl); }};

  ProgressData progress_data{.abort_check_ = std::move(abort_check)};

  constexpr auto write_callback = [](char *ptr, size_t size, size_t nmemb, void *userdata) -> size_t {
    auto const *sink = static_cast<WriteSink const *>(userdata);
    return (*sink)(ptr, size * nmemb);
  };

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &write);
  // Timeout for establishing a connection
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connection_timeout);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
  curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.c_str());
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10);
  // Needed so that XFERINFOFUNCTION could work
  curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);
  curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progress_data);
  curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, DownloadProgressCb);
  // Fail fast on HTTP errors, so an error page is never handed to the sink
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
  SetCaInfo(curl);

  if (auto const res = curl_easy_perform(curl); res != CURLE_OK) {
    spdlog::error("Error happened while downloading {}: {}", url, curl_easy_strerror(res));
    return false;
  }

  return true;
}

auto DownloadToStream(std::string_view url, std::ostream &os) -> bool {
  constexpr auto WriteCallback = [](char *ptr, size_t size, size_t nmemb, std::ostream *os) -> size_t {
    auto const totalSize = static_cast<std::streamsize>(size * nmemb);
    os->write(ptr, totalSize);
    return totalSize;
  };

  auto *curl_handle{curl_easy_init()};
  // NOLINTNEXTLINE
  curl_easy_setopt(curl_handle, CURLOPT_URL, url.data());
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, +WriteCallback);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &os);
  SetCaInfo(curl_handle);

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

auto UrlToStringStream(std::string_view url) -> std::stringstream {
  auto ss = std::stringstream{};
  if (!requests::DownloadToStream(url, ss)) {
    throw utils::BasicException("CSV was unable to be fetched from {}", url);
  }
  return ss;
};

}  // namespace memgraph::requests
