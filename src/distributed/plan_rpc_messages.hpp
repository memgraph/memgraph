#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/rpc/messages.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace distributed {

const std::string kDistributedPlanServerName = "DistributedPlanRpc";

using communication::rpc::Message;
using SymbolTable = query::SymbolTable;
using AstTreeStorage = query::AstTreeStorage;

struct DispatchPlanReq : public Message {
  DispatchPlanReq() {}
  DispatchPlanReq(int64_t plan_id,
                  std::shared_ptr<query::plan::LogicalOperator> plan,
                  SymbolTable symbol_table)

      : plan_id_(plan_id), plan_(plan), symbol_table_(symbol_table) {}
  int64_t plan_id_;
  std::shared_ptr<query::plan::LogicalOperator> plan_;
  SymbolTable symbol_table_;
  AstTreeStorage storage_;

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar &boost::serialization::base_object<Message>(*this);
    ar &plan_id_;
    ar &plan_;
    ar &symbol_table_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &plan_id_;
    ar &plan_;
    ar &symbol_table_;
    storage_ = std::move(
        ar.template get_helper<AstTreeStorage>(AstTreeStorage::kHelperId));
  }
};

RPC_SINGLE_MEMBER_MESSAGE(ConsumePlanRes, bool);

using DistributedPlanRpc =
    communication::rpc::RequestResponse<DispatchPlanReq, ConsumePlanRes>;

}  // namespace distributed
