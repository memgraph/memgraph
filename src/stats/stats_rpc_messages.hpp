#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/string.hpp"
#include "boost/serialization/utility.hpp"
#include "boost/serialization/vector.hpp"

#include "communication/rpc/messages.hpp"
#include "utils/datetime/timestamp.hpp"

namespace stats {

struct StatsReq : public communication::rpc::Message {
  StatsReq() {}
  StatsReq(std::string metric_path,
           std::vector<std::pair<std::string, std::string>> tags, double value)
      : metric_path(metric_path),
        tags(tags),
        value(value),
        timestamp(Timestamp::Now().SecSinceTheEpoch()) {}

  std::string metric_path;
  std::vector<std::pair<std::string, std::string>> tags;
  double value;
  uint64_t timestamp;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &metric_path &tags &value &timestamp;
  }
};

RPC_NO_MEMBER_MESSAGE(StatsRes);

struct BatchStatsReq : public communication::rpc::Message {
  BatchStatsReq() {}
  BatchStatsReq(std::vector<StatsReq> requests) : requests(requests) {}

  std::vector<StatsReq> requests;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &requests;
  }
};

RPC_NO_MEMBER_MESSAGE(BatchStatsRes);

using StatsRpc = communication::rpc::RequestResponse<StatsReq, StatsRes>;
using BatchStatsRpc =
    communication::rpc::RequestResponse<BatchStatsReq, BatchStatsRes>;

}  // namespace stats
