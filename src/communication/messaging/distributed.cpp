#include "communication/messaging/distributed.hpp"

namespace communication::messaging {

System::System(const std::string &address, uint16_t port)
    : endpoint_(address, port) {
  // Numbers of worker are quite arbitrary at the point.
  StartClient(4);
  StartServer(4);
}

System::~System() {
  for (size_t i = 0; i < pool_.size(); ++i) {
    pool_[i].join();
  }
  thread_.join();
}

void System::Shutdown() {
  queue_.Shutdown();
  server_->Shutdown();
}

void System::StartClient(int worker_count) {
  LOG(INFO) << "Starting " << worker_count << " client workers";
  for (int i = 0; i < worker_count; ++i) {
    pool_.push_back(std::thread([this]() {
      while (true) {
        auto message = queue_.AwaitPop();
        if (message == std::experimental::nullopt) break;
        SendMessage(message->address, message->port, message->channel,
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
  server_ = std::make_unique<ServerT>(endpoint_, protocol_data_);
  endpoint_ = server_->endpoint();

  // Start server.
  thread_ = std::thread(
      [worker_count, this]() { this->server_->Start(worker_count); });
}

std::shared_ptr<EventStream> System::Open(const std::string &name) {
  return system_.Open(name);
}

Writer::Writer(System &system, const std::string &address, uint16_t port,
               const std::string &name)
    : system_(system), address_(address), port_(port), name_(name) {}

void Writer::Send(std::unique_ptr<Message> message) {
  system_.queue_.Emplace(address_, port_, name_, std::move(message));
}
}
