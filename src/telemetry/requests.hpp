#pragma once

#include <string>

#include <json/json.hpp>

namespace telemetry {

/**
 * This function is called by the main telemetry `Init` function to initialize
 * the requests library.
 */
void RequestsInit();

/**
 * This function sends a POST request with a JSON payload to the `url`.
 *
 * @param url url to which to send the request
 * @param data json payload
 * @param timeout the timeout that should be used when making the request
 * @return bool true if the request was successful, false otherwise.
 */
bool RequestPostJson(const std::string &url, const nlohmann::json &data,
                     const int timeout = 10);

}  // namespace telemetry
