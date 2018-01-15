#include "communication/messaging/distributed.hpp"

namespace communication::messaging {

System::System(const io::network::Endpoint &endpoint) : endpoint_(endpoint) {
  // Numbers of workers is quite arbitrary at this point.
  StartClient(4);
  StartServer(4);
}

System::~System() {
  queue_.Shutdown();
  for (size_t i = 0; i < pool_.size(); ++i) {
    pool_[i].join();
  }
}

void System::StartClient(int worker_count) {
  LOG(INFO) << "Starting " << worker_count << " client workers";
  for (int i = 0; i < worker_count; ++i) {
    pool_.push_back(std::thread([this]() {
      while (true) {
        auto message = queue_.AwaitPop();
        if (message == std::experimental::nullopt) break;
        SendMessage(message->endpoint, message->channel,
                    std::move(message->message));
      }
    }));
  }
}

void System::StartServer(int worker_count) {
  if (server_ != nullptr) {
    LOG(FATAL) << "Tried to start a running server!";
  }

  // Initialize server.
  server_ = std::make_unique<ServerT>(endpoint_, protocol_data_, worker_count);
  endpoint_ = server_->endpoint();
}

std::shared_ptr<EventStream> System::Open(const std::string &name) {
  return system_.Open(name);
}

Writer::Writer(System &system, const Endpoint &endpoint,
               const std::string &name)
    : system_(system), endpoint_(endpoint), name_(name) {}

void Writer::Send(std::unique_ptr<Message> message) {
  system_.queue_.Emplace(endpoint_, name_, std::move(message));
}
}  // namespace communication::messaging
