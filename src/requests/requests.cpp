// Copyright 2023 Memgraph Ltd.
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
#include <ctre/ctre.hpp>

#include "utils/logging.hpp"

namespace memgraph::requests {

namespace {

size_t CurlWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) { return nmemb; }

}  // namespace

void Init() { curl_global_init(CURL_GLOBAL_ALL); }

bool RequestPostJson(const std::string &url, const nlohmann::json &data, int timeout_in_seconds) {
  CURL *curl = nullptr;
  CURLcode res = CURLE_UNSUPPORTED_PROTOCOL;

  long response_code = 0;
  struct curl_slist *headers = NULL;
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

bool CreateAndDownloadFile(const std::string &url, const std::string &path, int timeout_in_seconds) {
  CURL *curl = nullptr;
  CURLcode res = CURLE_UNSUPPORTED_PROTOCOL;

  long response_code = 0;
  std::string user_agent = fmt::format("memgraph/{}", gflags::VersionString());

  curl = curl_easy_init();
  if (!curl) return false;

  FILE *file = std::fopen(path.c_str(), "w");
  if (!file) return false;

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
  curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_in_seconds);

  res = curl_easy_perform(curl);
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl);
  std::fclose(file);

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

}  // namespace memgraph::requests
