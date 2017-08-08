#include "communication.hpp"

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  System system;
  system.network().StartClient(1);
  auto channel = system.network().Resolve("127.0.0.1", 10000, "master", "main");
  std::cout << channel << std::endl;
  if (channel != nullptr) {
    auto message = std::make_unique<SenderMessage>("master", "main");
    channel->SendHelper(typeid(SenderMessage), std::move(message));
  }
  system.network().StopClient();
  return 0;
}
