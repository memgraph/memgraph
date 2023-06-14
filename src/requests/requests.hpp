// Copyright 2022 Memgraph Ltd.
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

#include <string>

#include <json/json.hpp>

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
 * @return bool true if the request was successful, false otherwise.
 */
bool RequestPostJson(const std::string &url, const nlohmann::json &data, int timeout_in_seconds = 10);

/**
 * This functions sends a GET request to the given `url` and writes the response
 * to the given `path`.
 *
 * @param url url to which to send the request
 * @param path path to the file where the response in written
 * @param timeout the timeout that should be used when making the request
 * @return bool true if the request was successful, false otherwise.
 */
bool CreateAndDownloadFile(const std::string &url, const std::string &path, int timeout_in_seconds = 10);

}  // namespace memgraph::requests
