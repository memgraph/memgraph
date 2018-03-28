#pragma once

#include <memory>
#include <type_traits>
#include <typeindex>

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

namespace communication::rpc {

using MessageSize = uint32_t;

/**
 * Base class for messages.
 */
class Message {
 public:
  virtual ~Message() {}

  /**
   * Run-time type identification that is used for callbacks.
   *
   * Warning: this works because of the virtual destructor, don't remove it from
   * this class
   */
  std::type_index type_index() const { return typeid(*this); }

 private:
  friend boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &, unsigned int) {}
};

template <typename TRequest, typename TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

}  // namespace communication::rpc

// RPC Pimp
#define RPC_NO_MEMBER_MESSAGE(name)                                       \
  struct name : public communication::rpc::Message {                      \
    name() {}                                                             \
                                                                          \
   private:                                                               \
    friend class boost::serialization::access;                            \
                                                                          \
    template <class TArchive>                                             \
    void serialize(TArchive &ar, unsigned int) {                          \
      ar &boost::serialization::base_object<communication::rpc::Message>( \
          *this);                                                         \
    }                                                                     \
  }

#define RPC_SINGLE_MEMBER_MESSAGE(name, type)                             \
  struct name : public communication::rpc::Message {                      \
    name() {}                                                             \
    name(const type &member) : member(member) {}                          \
    type member;                                                          \
                                                                          \
   private:                                                               \
    friend class boost::serialization::access;                            \
                                                                          \
    template <class TArchive>                                             \
    void serialize(TArchive &ar, unsigned int) {                          \
      ar &boost::serialization::base_object<communication::rpc::Message>( \
          *this);                                                         \
      ar &member;                                                         \
    }                                                                     \
  }
