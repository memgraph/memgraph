#include "reactors_distributed.hpp"

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  Distributed &distributed = Distributed::GetInstance();
  distributed.network().StartClient(1);
  auto channel = distributed.network().Resolve("127.0.0.1", 10000, "master", "main");
  std::cout << channel << std::endl;
  if (channel != nullptr) {
    channel->Send<ReturnAddressMsg>("master", "main");
  }
  distributed.network().StopClient();
  return 0;
}
