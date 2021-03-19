#include <gflags/gflags.h>
#include <mgclient.hpp>

#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(timeout, 120, "Timeout seconds");

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Memory Control");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();

  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  if (!client) {
    LOG_FATAL("Failed to connect!");
  }

  client->Execute("MATCH (n) DETACH DELETE n;");
  client->DiscardAll();

  const auto *create_query = "UNWIND range(1, 50) as u CREATE (n {string: \"Some longer string\"}) RETURN n;";

  utils::Timer timer;
  while (true) {
    if (timer.Elapsed<std::chrono::duration<uint64_t>>().count() > FLAGS_timeout) {
      LOG_FATAL("The test timed out");
    }
    client->Execute(create_query);
    if (!client->FetchOne()) {
      break;
    }
    client->DiscardAll();
  }

  spdlog::info("Memgraph is out of memory");

  spdlog::info("Cleaning up unused memory");
  client->Execute("MATCH (n) DETACH DELETE n;");
  client->DiscardAll();
  client->Execute("FREE MEMORY;");
  client->DiscardAll();

  // now it should succeed
  spdlog::info("Retrying the query with the memory cleaned up");
  client->Execute(create_query);
  if (!client->FetchOne()) {
    LOG_FATAL("Memgraph is still out of memory");
  }

  return 0;
}
