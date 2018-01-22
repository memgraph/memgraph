#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/messaging/local.hpp"
#include "communication/rpc/rpc.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "utils/rpc_pimp.hpp"

namespace distributed {

const std::string kDistributedPlanServerName = "DistributedPlanRpc";

using communication::messaging::Message;
using SymbolTable = query::SymbolTable;

struct DispatchPlanReq : public Message {
  DispatchPlanReq() {}
  DispatchPlanReq(int64_t plan_id,
                  std::shared_ptr<query::plan::LogicalOperator> plan,
                  SymbolTable symbol_table)
      : plan_id(plan_id), plan(plan), symbol_table(symbol_table) {}
  int64_t plan_id;
  std::shared_ptr<query::plan::LogicalOperator> plan;
  SymbolTable symbol_table;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &plan_id;
    ar &plan;
    ar &symbol_table;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(ConsumePlanRes, bool);

using DistributePlan =
    communication::rpc::RequestResponse<DispatchPlanReq, ConsumePlanRes>;

}  // namespace distributed
