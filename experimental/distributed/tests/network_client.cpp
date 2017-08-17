#include "communication.hpp"

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  System system;
  Distributed distributed(system);
  distributed.network().StartClient(1);
  auto channel = distributed.network().Resolve("127.0.0.1", 10000, "master", "main");
  std::cout << channel << std::endl;
  if (channel != nullptr) {
    channel->Send<SenderMessage>("master", "main");
  }
  distributed.network().StopClient();
  return 0;
}
