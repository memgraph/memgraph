#pragma once

#include "cereal/archives/binary.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/vector.hpp"

#include "communication/messaging/local.hpp"

#define RPC_NO_MEMBER_MESSAGE(name)                    \
  using communication::messaging::Message;             \
  struct name : public Message {                       \
    name() {}                                          \
    template <class Archive>                           \
    void serialize(Archive &ar) {                      \
      ar(::cereal::virtual_base_class<Message>(this)); \
    }                                                  \
  };

#define RPC_SINGLE_MEMBER_MESSAGE(name, type)                  \
  using communication::messaging::Message;                     \
  struct name : public Message {                               \
    name() {}                                                  \
    name(const type &member) : member(member) {}               \
    type member;                                               \
    template <class Archive>                                   \
    void serialize(Archive &ar) {                              \
      ar(::cereal::virtual_base_class<Message>(this), member); \
    }                                                          \
  };

