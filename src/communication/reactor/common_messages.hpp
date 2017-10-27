#pragma once

#include "communication/reactor/reactor_local.hpp"

// TODO: Which of these I need to include.
#include "cereal/archives/binary.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/vector.hpp"

DECLARE_string(reactor_address);
DECLARE_int32(reactor_port);

namespace communication::reactor {

/**
 * Message that includes the channel on which response is expected;
 */
class ReturnAddressMessage : public Message {
 public:
  ReturnAddressMessage(std::string reactor, std::string channel)
      : address_(FLAGS_reactor_address),
        port_(FLAGS_reactor_port),
        reactor_(reactor),
        channel_(channel) {}

  const std::string &address() const { return address_; }
  uint16_t port() const { return port_; }
  const std::string &reactor_name() const { return reactor_; }
  const std::string &channel_name() const { return channel_; }

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), address_, port_, reactor_,
       channel_);
  }

  auto FindChannel(ChannelFinder &finder) const {
    return finder.FindChannel(address_, port_, reactor_, channel_);
  }

 protected:
  friend class cereal::access;
  ReturnAddressMessage() {}  // Cereal needs access to a default constructor.

  // Good luck these being const using cereal...
  std::string address_;
  uint16_t port_;
  std::string reactor_;
  std::string channel_;
};
}
CEREAL_REGISTER_TYPE(communication::reactor::ReturnAddressMessage);
