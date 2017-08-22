#include "reactors_distributed.hpp"

DEFINE_string(address, "127.0.0.1", "Network server bind address");
DEFINE_int32(port, 10000, "Network server bind port");

Network::Network() {}

/**
 * SenderMessage implementation.
 */
SenderMessage::SenderMessage() {}

SenderMessage::SenderMessage(std::string reactor, std::string channel)
    : address_(FLAGS_address),
      port_(FLAGS_port),
      reactor_(reactor),
      channel_(channel) {}

std::string SenderMessage::Address() const { return address_; }
uint16_t SenderMessage::Port() const { return port_; }
std::string SenderMessage::ReactorName() const { return reactor_; }
std::string SenderMessage::ChannelName() const { return channel_; }

std::shared_ptr<Channel> SenderMessage::GetChannelToSender(
    Distributed *distributed) const {
  if (address_ == FLAGS_address && port_ == FLAGS_port) {
    return System::GetInstance().FindChannel(reactor_, channel_);
  }
  if (distributed)
    return distributed->network().Resolve(address_, port_, reactor_, channel_);
  assert(false);
}
