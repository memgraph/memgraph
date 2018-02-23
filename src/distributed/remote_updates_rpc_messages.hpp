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
enum class RemoteUpdateResult {
  DONE,
  SERIALIZATION_ERROR,
  LOCK_TIMEOUT_ERROR,
  UPDATE_DELETED_ERROR
};

RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateReq, database::StateDelta);
RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateRes, RemoteUpdateResult);
using RemoteUpdateRpc =
    communication::rpc::RequestResponse<RemoteUpdateReq, RemoteUpdateRes>;

RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateApplyReq, tx::transaction_id_t);
RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateApplyRes, RemoteUpdateResult);
using RemoteUpdateApplyRpc =
    communication::rpc::RequestResponse<RemoteUpdateApplyReq,
                                        RemoteUpdateApplyRes>;

struct RemoteCreateResult {
  RemoteUpdateResult result;
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

struct RemoteCreateVertexReqData {
  tx::transaction_id_t tx_id;
  std::vector<storage::Label> labels;
  std::unordered_map<storage::Property, query::TypedValue> properties;

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
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

RPC_SINGLE_MEMBER_MESSAGE(RemoteCreateVertexReq, RemoteCreateVertexReqData);
RPC_SINGLE_MEMBER_MESSAGE(RemoteCreateVertexRes, RemoteCreateResult);
using RemoteCreateVertexRpc =
    communication::rpc::RequestResponse<RemoteCreateVertexReq,
                                        RemoteCreateVertexRes>;

struct RemoteCreateEdgeReqData {
  gid::Gid from;
  storage::VertexAddress to;
  storage::EdgeType edge_type;
  tx::transaction_id_t tx_id;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &from;
    ar &to;
    ar &edge_type;
    ar &tx_id;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(RemoteCreateEdgeReq, RemoteCreateEdgeReqData);
RPC_SINGLE_MEMBER_MESSAGE(RemoteCreateEdgeRes, RemoteCreateResult);
using RemoteCreateEdgeRpc =
    communication::rpc::RequestResponse<RemoteCreateEdgeReq,
                                        RemoteCreateEdgeRes>;

struct RemoteAddInEdgeReqData {
  storage::VertexAddress from;
  storage::EdgeAddress edge_address;
  gid::Gid to;
  storage::EdgeType edge_type;
  tx::transaction_id_t tx_id;

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

RPC_SINGLE_MEMBER_MESSAGE(RemoteAddInEdgeReq, RemoteAddInEdgeReqData);
RPC_SINGLE_MEMBER_MESSAGE(RemoteAddInEdgeRes, RemoteUpdateResult);
using RemoteAddInEdgeRpc =
    communication::rpc::RequestResponse<RemoteAddInEdgeReq, RemoteAddInEdgeRes>;
}  // namespace distributed
