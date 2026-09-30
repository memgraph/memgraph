#include "pulsar/Client.h"

using namespace pulsar;

int main() {
  const std::string topic = "test-server-connect-error";
  Client client("pulsar://localhost:65535", ClientConfiguration().setOperationTimeoutSeconds(1));
  Producer producer;
  return 0;
}
