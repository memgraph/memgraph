#pragma once

#include <unordered_map>

#include "boost/serialization/vector.hpp"

#include "communication/rpc/messages.hpp"
#include "database/state_delta.hpp"
#include "storage/address_types.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"
#include "utils/serialization.hpp"

namespace distributed {

/// The result of sending or applying a deferred update to a worker.
enum class UpdateResult {
  DONE,
  SERIALIZATION_ERROR,
  LOCK_TIMEOUT_ERROR,
  UPDATE_DELETED_ERROR,
  UNABLE_TO_DELETE_VERTEX_ERROR
};

RPC_SINGLE_MEMBER_MESSAGE(UpdateReq, database::StateDelta);
RPC_SINGLE_MEMBER_MESSAGE(UpdateRes, UpdateResult);
using UpdateRpc = communication::rpc::RequestResponse<UpdateReq, UpdateRes>;

RPC_SINGLE_MEMBER_MESSAGE(UpdateApplyReq, tx::TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(UpdateApplyRes, UpdateResult);
using UpdateApplyRpc =
    communication::rpc::RequestResponse<UpdateApplyReq, UpdateApplyRes>;

struct CreateResult {
  UpdateResult result;
  // Only valid if creation was successful.
  gid::Gid gid;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &result;
    ar &gid;
  }
};

struct CreateVertexReqData {
  tx::TransactionId tx_id;
  std::vector<storage::Label> labels;
  std::unordered_map<storage::Property, query::TypedValue> properties;
  std::experimental::optional<gid::Gid> requested_gid;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << tx_id;
    ar << labels;
    ar << properties.size();
    for (auto &kv : properties) {
      ar << kv.first;
      utils::SaveTypedValue(ar, kv.second);
    }
    ar << requested_gid;
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> tx_id;
    ar >> labels;
    size_t props_size;
    ar >> props_size;
    for (size_t i = 0; i < props_size; ++i) {
      storage::Property p;
      ar >> p;
      query::TypedValue tv;
      utils::LoadTypedValue(ar, tv);
      properties.emplace(p, std::move(tv));
    }
    ar >> requested_gid;
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

RPC_SINGLE_MEMBER_MESSAGE(CreateVertexReq, CreateVertexReqData);
RPC_SINGLE_MEMBER_MESSAGE(CreateVertexRes, CreateResult);
using CreateVertexRpc =
    communication::rpc::RequestResponse<CreateVertexReq, CreateVertexRes>;

struct CreateEdgeReqData {
  gid::Gid from;
  storage::VertexAddress to;
  storage::EdgeType edge_type;
  tx::TransactionId tx_id;
  std::experimental::optional<gid::Gid> requested_gid;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &from;
    ar &to;
    ar &edge_type;
    ar &tx_id;
    ar &requested_gid;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(CreateEdgeReq, CreateEdgeReqData);
RPC_SINGLE_MEMBER_MESSAGE(CreateEdgeRes, CreateResult);
using CreateEdgeRpc =
    communication::rpc::RequestResponse<CreateEdgeReq, CreateEdgeRes>;

struct AddInEdgeReqData {
  storage::VertexAddress from;
  storage::EdgeAddress edge_address;
  gid::Gid to;
  storage::EdgeType edge_type;
  tx::TransactionId tx_id;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &from;
    ar &edge_address;
    ar &to;
    ar &edge_type;
    ar &tx_id;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(AddInEdgeReq, AddInEdgeReqData);
RPC_SINGLE_MEMBER_MESSAGE(AddInEdgeRes, UpdateResult);
using AddInEdgeRpc =
    communication::rpc::RequestResponse<AddInEdgeReq, AddInEdgeRes>;

struct RemoveVertexReqData {
  gid::Gid gid;
  tx::TransactionId tx_id;
  bool check_empty;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &gid;
    ar &tx_id;
    ar &check_empty;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(RemoveVertexReq, RemoveVertexReqData);
RPC_SINGLE_MEMBER_MESSAGE(RemoveVertexRes, UpdateResult);
using RemoveVertexRpc =
    communication::rpc::RequestResponse<RemoveVertexReq, RemoveVertexRes>;

struct RemoveEdgeData {
  tx::TransactionId tx_id;
  gid::Gid edge_id;
  gid::Gid vertex_from_id;
  storage::VertexAddress vertex_to_address;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &tx_id;
    ar &edge_id;
    ar &vertex_from_id;
    ar &vertex_to_address;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(RemoveEdgeReq, RemoveEdgeData);
RPC_SINGLE_MEMBER_MESSAGE(RemoveEdgeRes, UpdateResult);
using RemoveEdgeRpc =
    communication::rpc::RequestResponse<RemoveEdgeReq, RemoveEdgeRes>;

struct RemoveInEdgeData {
  tx::TransactionId tx_id;
  gid::Gid vertex;
  storage::EdgeAddress edge_address;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &tx_id;
    ar &vertex;
    ar &edge_address;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(RemoveInEdgeReq, RemoveInEdgeData);
RPC_SINGLE_MEMBER_MESSAGE(RemoveInEdgeRes, UpdateResult);
using RemoveInEdgeRpc =
    communication::rpc::RequestResponse<RemoveInEdgeReq, RemoveInEdgeRes>;

}  // namespace distributed
