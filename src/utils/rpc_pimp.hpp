#pragma once

#include "boost/serialization/base_object.hpp"

#include "communication/messaging/local.hpp"

#define RPC_NO_MEMBER_MESSAGE(name)                        \
  struct name : public communication::messaging::Message { \
    name() {}                                              \
                                                           \
   private:                                                \
    friend class boost::serialization::access;             \
                                                           \
    template <class TArchive>                              \
    void serialize(TArchive &ar, unsigned int) {           \
      ar &boost::serialization::base_object<               \
          communication::messaging::Message>(*this);       \
    }                                                      \
  };

#define RPC_SINGLE_MEMBER_MESSAGE(name, type)              \
  struct name : public communication::messaging::Message { \
    name() {}                                              \
    name(const type &member) : member(member) {}           \
    type member;                                           \
                                                           \
   private:                                                \
    friend class boost::serialization::access;             \
                                                           \
    template <class TArchive>                              \
    void serialize(TArchive &ar, unsigned int) {           \
      ar &boost::serialization::base_object<               \
          communication::messaging::Message>(*this);       \
      ar &member;                                          \
    }                                                      \
  };
