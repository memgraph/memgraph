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

#pragma once

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <nlohmann/json_fwd.hpp>
#include <ostream>
#include <string>
#include <string_view>

#include "utils/file.hpp"

namespace memgraph::requests {

/**
 * Call this function in each `main` file that uses the Requests stack. It is
 * used to initialize all libraries (primarily cURL).
 *
 * NOTE: This function must be called **exactly** once.
 */
void Init();

/**
 *
 * This function sends a POST request with a JSON payload to the `url`.
 *
 * @param url url to which to send the request
 * @param data json payload
 * @param timeout the timeout that should be used when making the request
 * @param abort_flag optional pointer to an atomic flag. If set and true, the
 *        in-progress transfer will be aborted via the progress callback.
 * @return bool true if the request was successful, false otherwise.
 */
bool RequestPostJson(const std::string &url, const nlohmann::json &data, int timeout_in_seconds = 10,
                     std::atomic<bool> const *abort_flag = nullptr);

/// Why a download did not deliver the whole body.
enum class DownloadFailure : uint8_t {
  /// Could not reach the server, or lost it part way through.
  Network,
  /// The server refused the request as written, and will keep refusing it.
  HttpClientError,
  /// The server turned this attempt away but invited another, such as while it is rate limiting.
  HttpTryAgain,
  /// The server failed to serve a request it accepted.
  HttpServerError,
  /// The destination would not take what was delivered, such as a full disk.
  LocalWrite,
  /// Nothing arrived for long enough that the transfer was given up on.
  Stalled,
};

/// What a status the server replied with says about repeating the request. A 5xx is the server
/// failing something it accepted, and 408 and 429 are the two 4xx that ask for the same request
/// again rather than a different one.
[[nodiscard]] constexpr auto ClassifyHttpStatus(int status) -> DownloadFailure {
  constexpr auto kRequestTimeout = 408;
  constexpr auto kTooManyRequests = 429;
  constexpr auto kServerError = 500;

  if (status >= kServerError) {
    return DownloadFailure::HttpServerError;
  }
  if (status == kRequestTimeout || status == kTooManyRequests) {
    return DownloadFailure::HttpTryAgain;
  }
  return DownloadFailure::HttpClientError;
}

struct DownloadError {
  DownloadFailure kind{DownloadFailure::Network};
  /// The status the server replied with, or 0 if it never replied.
  int http_status{0};
  std::string message;

  /// Whether making the same request again could produce a different outcome. Deciding this is HTTP's
  /// business, so it is settled here rather than by each caller.
  [[nodiscard]] auto Retryable() const -> bool {
    return kind == DownloadFailure::Network || kind == DownloadFailure::HttpServerError ||
           kind == DownloadFailure::HttpTryAgain || kind == DownloadFailure::Stalled;
  }
};

/// Sends a GET request to `url` and writes the response body into `file`, which is closed when the
/// call returns. Reports why the body was not delivered in full.
std::expected<void, DownloadError> CreateAndDownloadFile(const std::string &url, utils::FileUniquePtr file,
                                                         uint64_t connection_timeout,
                                                         std::function<void()> abort_check = nullptr);

/// Receives a block of the response body and returns how many of those bytes it took. Taking fewer
/// than offered ends the transfer.
using WriteSink = std::function<size_t(char const *, size_t)>;

/// How long a body may go on arriving at under a byte a second before the transfer is treated as
/// stalled. Long enough that a slow but living server is left alone.
inline constexpr uint64_t kDefaultStallWindowSec = 30;

/**
 * Sends a GET request to `url` and hands the response body to `write` as it arrives, without ever
 * holding the whole body.
 *
 * @param url url to which to send the request
 * @param write receives each block of the body. Returning less than it was given aborts the
 *        transfer, which is how a reader that has stopped consuming brings the download to an end.
 * @param connection_timeout the timeout that should be used when making the request. The default
 *        timeout of 0 would use built-in connection timeout of 300s.
 * @param abort_check called periodically while the transfer is in progress, and signals by throwing
 * @param stall_window_sec how long the body may go on arriving at under a byte a second before the
 *        transfer is given up on as stalled. Measured against what the server delivers, so a `write`
 *        that takes its time does not count towards it.
 * @return nothing if the whole body was delivered, otherwise why it was not
 */
std::expected<void, DownloadError> DownloadToSink(const std::string &url, WriteSink const &write,
                                                  uint64_t connection_timeout,
                                                  std::function<void()> abort_check = nullptr,
                                                  uint64_t stall_window_sec = kDefaultStallWindowSec);

/**
 * Downloads content into a stream
 *
 * This function sends a GET request an put the response within a stream.
 * Using c-string because internals interop with a C API
 *
 * @param url url of the contents
 * @param os an output stream
 * @return bool true if the request was successful, false otherwise.
 */
auto DownloadToStream(std::string_view url, std::ostream &os) -> bool;

/**
 * Downloads content into a stream by calling DownloadToStream.

 * @param url url of the contents
 * @return std::stringstream containing the content of the fetched file
 */
auto UrlToStringStream(std::string_view url) -> std::stringstream;

}  // namespace memgraph::requests
