#pragma once
#include <optional>
#include <string>

namespace storage::replication {
struct ReplicationClientConfig {
  std::optional<double> timeout;

  struct SSL {
    std::string key_file = "";
    std::string cert_file = "";
  };

  std::optional<SSL> ssl;
};

struct ReplicationServerConfig {
  struct SSL {
    std::string key_file;
    std::string cert_file;
    std::string ca_file;
    bool verify_peer;
  };

  std::optional<SSL> ssl;
};
}  // namespace storage::replication
