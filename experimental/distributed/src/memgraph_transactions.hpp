#pragma once

#include <string>

#include "reactors_local.hpp"
#include "reactors_distributed.hpp"

/**
 * Message which encapsulates query.
 * It is create on Client and sent to Master which will process it.
 */
class QueryMsg : public ReturnAddressMsg {
 public:
  QueryMsg(std::string return_channel, std::string query)
      : ReturnAddressMsg(return_channel), query_(query) {}

  const std::string &query() const { return query_; }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<ReturnAddressMsg>(this), query_);
  }

protected:
  // Cereal needs access to default constructor.
  friend class cereal::access;
  QueryMsg() = default;

  std::string query_;
};
CEREAL_REGISTER_TYPE(QueryMsg);

/**
 * Message which encapuslates result of a query.
 * Currently, result is string.
 */
class ResultMsg : public Message {
 public:
  ResultMsg(std::string result) : result_(result) {}

  const std::string &result() const { return result_; }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<Message>(this), result_);
  }

 protected:
  friend class cereal::access;
  ResultMsg() = default;

  std::string result_;
};
CEREAL_REGISTER_TYPE(ResultMsg);

/**
 * Below are message that are exchanged between Master and Workers.
 */

class QueryCreateVertex : public ReturnAddressMsg {
public:
  QueryCreateVertex(std::string return_channel)
      : ReturnAddressMsg(return_channel) {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<ReturnAddressMsg>(this));
  }

protected:
  // Cereal needs access to default constructor.
  friend class cereal::access;
  QueryCreateVertex() {}
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


class QueryVertexCount : public ReturnAddressMsg {
 public:
  QueryVertexCount(std::string return_channel)
      : ReturnAddressMsg(return_channel) {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<ReturnAddressMsg>(this));
  }

 protected:
   // Cereal needs access to default constructor.
  friend class cereal::access;
  QueryVertexCount() {}
};
CEREAL_REGISTER_TYPE(QueryVertexCount);

class ResultQueryVertexCount : public Message {
 public:
  ResultQueryVertexCount(int64_t count) : count_(count) {}

  int64_t count() const { return count_; }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<Message>(this), count_);
  }

 protected:
  // Cereal needs access to default constructor.
  friend class cereal::access;
  ResultQueryVertexCount() {}

  int64_t count_;
};
CEREAL_REGISTER_TYPE(ResultQueryVertexCount);