#include "database/counters.hpp"

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/utility.hpp"

namespace database {

const std::string kCountersRpc = "CountersRpc";

RPC_SINGLE_MEMBER_MESSAGE(CountersGetReq, std::string);
RPC_SINGLE_MEMBER_MESSAGE(CountersGetRes, int64_t);
using CountersGetRpc =
    communication::rpc::RequestResponse<CountersGetReq, CountersGetRes>;

using CountersSetReqData = std::pair<std::string, int64_t>;
RPC_SINGLE_MEMBER_MESSAGE(CountersSetReq, CountersSetReqData);
RPC_NO_MEMBER_MESSAGE(CountersSetRes);
using CountersSetRpc =
    communication::rpc::RequestResponse<CountersSetReq, CountersSetRes>;

int64_t SingleNodeCounters::Get(const std::string &name) {
  return counters_.access()
      .emplace(name, std::make_tuple(name), std::make_tuple(0))
      .first->second.fetch_add(1);
}

void SingleNodeCounters::Set(const std::string &name, int64_t value) {
  auto name_counter_pair = counters_.access().emplace(
      name, std::make_tuple(name), std::make_tuple(value));
  if (!name_counter_pair.second) name_counter_pair.first->second.store(value);
}

MasterCounters::MasterCounters(communication::rpc::System &system)
    : rpc_server_(system, kCountersRpc) {
  rpc_server_.Register<CountersGetRpc>([this](const CountersGetReq &req) {
    return std::make_unique<CountersGetRes>(Get(req.member));
  });
  rpc_server_.Register<CountersSetRpc>([this](const CountersSetReq &req) {
    Set(req.member.first, req.member.second);
    return std::make_unique<CountersSetRes>();
  });
}

WorkerCounters::WorkerCounters(const io::network::Endpoint &master_endpoint)
    : rpc_client_(master_endpoint, kCountersRpc) {}

int64_t WorkerCounters::Get(const std::string &name) {
  auto response = rpc_client_.Call<CountersGetRpc>(name);
  CHECK(response) << "CountersGetRpc - failed to get response from master";
  return response->member;
}

void WorkerCounters::Set(const std::string &name, int64_t value) {
  auto response =
      rpc_client_.Call<CountersSetRpc>(CountersSetReqData{name, value});
  CHECK(response) << "CountersSetRpc - failed to get response from master";
}

}  // namespace database

BOOST_CLASS_EXPORT(database::CountersGetReq);
BOOST_CLASS_EXPORT(database::CountersGetRes);
BOOST_CLASS_EXPORT(database::CountersSetReq);
BOOST_CLASS_EXPORT(database::CountersSetRes);
