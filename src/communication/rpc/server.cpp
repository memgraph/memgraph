#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/unique_ptr.hpp"

#include "communication/rpc/server.hpp"

namespace communication::rpc {

System::System(const io::network::Endpoint &endpoint, const size_t worker_count)
    : server_(endpoint, *this, worker_count) {}

System::~System() {}

void System::AddTask(std::shared_ptr<Socket> socket, const std::string &service,
                     uint64_t message_id, std::unique_ptr<Message> message) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto it = services_.find(service);
  if (it == services_.end()) return;
  it->second->queue_.Emplace(std::move(socket), message_id, std::move(message));
}

void System::Add(Server &server) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto got = services_.emplace(server.service_name(), &server);
  CHECK(got.second) << fmt::format("Server with name {} already exists",
                                   server.service_name());
}

void System::Remove(const Server &server) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto it = services_.find(server.service_name());
  CHECK(it != services_.end()) << "Trying to delete nonexisting server";
  services_.erase(it);
}

Server::Server(System &system, const std::string &service_name,
               int workers_count)
    : system_(system), service_name_(service_name) {
  system_.Add(*this);
  for (int i = 0; i < workers_count; ++i) {
    threads_.push_back(std::thread([this]() {
      // TODO: Add logging.
      while (alive_) {
        auto task = queue_.AwaitPop();
        if (!task) continue;
        auto socket = std::move(std::get<0>(*task));
        auto message_id = std::get<1>(*task);
        auto message = std::move(std::get<2>(*task));
        auto callbacks_accessor = callbacks_.access();
        auto it = callbacks_accessor.find(message->type_index());
        if (it == callbacks_accessor.end()) continue;
        auto response = it->second(*(message.get()));
        SendMessage(*socket, message_id, response);
      }
    }));
  }
}

Server::~Server() {
  alive_.store(false);
  queue_.Shutdown();
  for (auto &thread : threads_) {
    if (thread.joinable()) thread.join();
  }
  system_.Remove(*this);
}

}  // namespace communication::rpc
