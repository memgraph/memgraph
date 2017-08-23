#include "reactors_distributed.hpp"

DEFINE_string(address, "127.0.0.1", "Network server bind address");
DEFINE_int32(port, 10000, "Network server bind port");

Network::Network() {}

/**
 * ReturnAddressMsg implementation.
 */
ReturnAddressMsg::ReturnAddressMsg() {}

ReturnAddressMsg::ReturnAddressMsg(std::string reactor, std::string channel)
    : address_(FLAGS_address),
      port_(FLAGS_port),
      reactor_(reactor),
      channel_(channel) {}

std::string ReturnAddressMsg::Address() const { return address_; }
uint16_t ReturnAddressMsg::Port() const { return port_; }
std::string ReturnAddressMsg::ReactorName() const { return reactor_; }
std::string ReturnAddressMsg::ChannelName() const { return channel_; }

std::shared_ptr<ChannelWriter> ReturnAddressMsg::GetReturnChannelWriter() const {
  if (address_ == FLAGS_address && port_ == FLAGS_port) {
    return System::GetInstance().FindChannel(reactor_, channel_);
  } else {
    // TODO(zuza): we should probably assert here if services have been already started.
    return Distributed::GetInstance().network().Resolve(address_, port_, reactor_, channel_);
  }
  assert(false);
}
