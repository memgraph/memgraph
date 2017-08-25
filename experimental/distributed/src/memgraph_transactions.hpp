#pragma once

#include "reactors_local.hpp"
#include "reactors_distributed.hpp"

class QueryCreateVertex : public ReturnAddressMsg {
public:
  QueryCreateVertex(std::string return_channel) : ReturnAddressMsg(return_channel) {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<ReturnAddressMsg>(this));
  }

protected:
  friend class cereal::access;
  QueryCreateVertex() {} // Cereal needs access to a default constructor.
};
CEREAL_REGISTER_TYPE(QueryCreateVertex);

class SuccessQueryCreateVertex : public Message {
public:
  SuccessQueryCreateVertex() {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<Message>(this));
  }
};
CEREAL_REGISTER_TYPE(SuccessQueryCreateVertex);


class FailureQueryCreateVertex : public Message {
public:
  FailureQueryCreateVertex() {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<Message>(this));
  }
};
CEREAL_REGISTER_TYPE(FailureQueryCreateVertex);
