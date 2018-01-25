#pragma once

#include <cstdint>
#include <string>

#include "boost/serialization/utility.hpp"
#include "boost/serialization/vector.hpp"

#include "communication/rpc/messages.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/parameters.hpp"
#include "transactions/type.hpp"
#include "utils/serialization.hpp"

namespace distributed {

/// The default number of results returned via RPC from remote execution to the
/// master that requested it.
constexpr int kDefaultBatchSize = 20;

/** Returnd along with a batch of results in the remote-pull RPC. Indicates the
 * state of execution on the worker. */
enum class RemotePullState {
  CURSOR_EXHAUSTED,
  CURSOR_IN_PROGRESS,
  SERIALIZATION_ERROR  // future-proofing for full CRUD
                       // TODO in full CRUD other errors
};

const std::string kRemotePullProduceRpcName = "RemotePullProduceRpc";

struct RemotePullReqData {
  tx::transaction_id_t tx_id;
  int64_t plan_id;
  Parameters params;
  std::vector<query::Symbol> symbols;
  int batch_size;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << tx_id;
    ar << plan_id;
    ar << params.size();
    for (auto &kv : params) {
      ar << kv.first;
      utils::SaveTypedValue(ar, kv.second);
    }
    ar << symbols;
    ar << batch_size;
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> tx_id;
    ar >> plan_id;
    size_t params_size;
    ar >> params_size;
    for (size_t i = 0; i < params_size; ++i) {
      int token_pos;
      ar >> token_pos;
      query::TypedValue param;
      utils::LoadTypedValue(ar, param);
      params.Add(token_pos, param);
    }
    ar >> symbols;
    ar >> batch_size;
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

struct RemotePullResData {
 public:
  RemotePullState pull_state;
  std::vector<std::vector<query::TypedValue>> frames;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << pull_state;
    ar << frames.size();
    // We need to indicate how many values are in each frame.
    // Assume all the frames have an equal number of elements.
    ar << (frames.size() == 0 ? 0 : frames[0].size());
    for (const auto &frame : frames)
      for (const auto &value : frame) {
        utils::SaveTypedValue(ar, value);
      }
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> pull_state;
    size_t frame_count;
    ar >> frame_count;
    size_t frame_size;
    ar >> frame_size;
    for (size_t i = 0; i < frame_count; ++i) {
      frames.emplace_back();
      auto &current_frame = frames.back();
      for (size_t j = 0; j < frame_size; ++j) {
        current_frame.emplace_back();
        utils::LoadTypedValue(ar, current_frame.back());
      }
    }
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

RPC_SINGLE_MEMBER_MESSAGE(RemotePullReq, RemotePullReqData);
RPC_SINGLE_MEMBER_MESSAGE(RemotePullRes, RemotePullResData);

using RemotePullRpc =
    communication::rpc::RequestResponse<RemotePullReq, RemotePullRes>;

// TODO make a separate RPC for the continuation of an existing pull, as an
// optimization not to have to send the full RemotePullReqData pack every time.

using EndRemotePullReqData = std::pair<tx::transaction_id_t, int64_t>;
RPC_SINGLE_MEMBER_MESSAGE(EndRemotePullReq, EndRemotePullReqData);
RPC_NO_MEMBER_MESSAGE(EndRemotePullRes);

using EndRemotePullRpc =
    communication::rpc::RequestResponse<EndRemotePullReq, EndRemotePullRes>;

}  // namespace distributed
