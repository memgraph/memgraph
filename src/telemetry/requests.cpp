#include "telemetry/requests.hpp"

#include <curl/curl.h>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

namespace telemetry {

void RequestsInit() { curl_global_init(CURL_GLOBAL_ALL); }

size_t CurlWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
  return nmemb;
}

bool RequestPostJson(const std::string &url, const nlohmann::json &data,
                     const int timeout) {
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
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);

  res = curl_easy_perform(curl);
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    DLOG(WARNING) << "Couldn't perform request: " << curl_easy_strerror(res);
    return false;
  }

  if (response_code != 200) {
    DLOG(WARNING) << "Request response code isn't 200 (received "
                  << response_code << ")!";
    return false;
  }

  return true;
}

}  // namespace telemetry
