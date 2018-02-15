#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "boost/serialization/utility.hpp"
#include "boost/serialization/vector.hpp"

#include "communication/rpc/messages.hpp"
#include "distributed/serialization.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/parameters.hpp"
#include "storage/address_types.hpp"
#include "transactions/type.hpp"
#include "utils/serialization.hpp"

namespace distributed {

/// The default number of results returned via RPC from remote execution to the
/// master that requested it.
constexpr int kDefaultBatchSize = 20;

/// Returnd along with a batch of results in the remote-pull RPC. Indicates the
/// state of execution on the worker.
enum class RemotePullState {
  CURSOR_EXHAUSTED,
  CURSOR_IN_PROGRESS,
  SERIALIZATION_ERROR,
  LOCK_TIMEOUT_ERROR,
  UPDATE_DELETED_ERROR,
  RECONSTRUCTION_ERROR,
  QUERY_ERROR
};

const std::string kRemotePullProduceRpcName = "RemotePullProduceRpc";

struct RemotePullReq : public communication::rpc::Message {
  RemotePullReq() {}
  RemotePullReq(tx::transaction_id_t tx_id, int64_t plan_id,
                const Parameters &params, std::vector<query::Symbol> symbols,
                bool accumulate, int batch_size, bool send_old, bool send_new)
      : tx_id(tx_id),
        plan_id(plan_id),
        params(params),
        symbols(symbols),
        accumulate(accumulate),
        batch_size(batch_size),
        send_old(send_old),
        send_new(send_new) {}

  tx::transaction_id_t tx_id;
  int64_t plan_id;
  Parameters params;
  std::vector<query::Symbol> symbols;
  bool accumulate;
  int batch_size;
  // Indicates which of (old, new) records of a graph element should be sent.
  bool send_old;
  bool send_new;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << boost::serialization::base_object<communication::rpc::Message>(*this);
    ar << tx_id;
    ar << plan_id;
    ar << params.size();
    for (auto &kv : params) {
      ar << kv.first;
      // Params never contain a vertex/edge, so save plan TypedValue.
      utils::SaveTypedValue(ar, kv.second);
    }
    ar << symbols;
    ar << accumulate;
    ar << batch_size;
    ar << send_old;
    ar << send_new;
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> boost::serialization::base_object<communication::rpc::Message>(*this);
    ar >> tx_id;
    ar >> plan_id;
    size_t params_size;
    ar >> params_size;
    for (size_t i = 0; i < params_size; ++i) {
      int token_pos;
      ar >> token_pos;
      query::TypedValue param;
      // Params never contain a vertex/edge, so load plan TypedValue.
      utils::LoadTypedValue(ar, param);
      params.Add(token_pos, param);
    }
    ar >> symbols;
    ar >> accumulate;
    ar >> batch_size;
    ar >> send_old;
    ar >> send_new;
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

/// The data returned to the end consumer (the RemotePull operator). Contains
/// only the relevant parts of the response, ready for use.
struct RemotePullData {
  RemotePullState pull_state;
  std::vector<std::vector<query::TypedValue>> frames;
};

/// The data of the remote pull response. Post-processing is required after
/// deserialization to initialize Vertex/Edge typed values in the frames
/// (possibly encapsulated in lists/maps) to their proper values. This requires
/// a GraphDbAccessor and therefore can't be done as part of deserialization.
///
/// TODO - make it possible to inject a &GraphDbAcessor from the RemotePull
/// layer
/// all the way into RPC data deserialization to remove the requirement for
/// post-processing. The current approach of holding references to parts of the
/// frame (potentially embedded in lists/maps) is too error-prone.
struct RemotePullResData {
 private:
  // Temp cache for deserialized vertices and edges. These objects are created
  // during deserialization. They are used immediatelly after during
  // post-processing. The vertex/edge data ownership gets transfered to the
  // RemoteCache, and the `element_in_frame` reference is used to set the
  // appropriate accessor to the appropriate value. Not used on side that
  // generates the response.
  template <typename TRecord>
  struct GraphElementData {
    using AddressT = storage::Address<mvcc::VersionList<TRecord>>;
    using PtrT = std::unique_ptr<TRecord>;

    GraphElementData(AddressT address, PtrT old_record, PtrT new_record,
                     query::TypedValue *element_in_frame)
        : global_address(address),
          old_record(std::move(old_record)),
          new_record(std::move(new_record)),
          element_in_frame(element_in_frame) {}

    storage::Address<mvcc::VersionList<TRecord>> global_address;
    std::unique_ptr<TRecord> old_record;
    std::unique_ptr<TRecord> new_record;
    // The position in frame is optional. This same structure is used for
    // deserializing path elements, in which case the vertex/edge in question is
    // not directly part of the frame.
    query::TypedValue *element_in_frame;
  };

  // Same like `GraphElementData`, but for paths.
  struct PathData {
    PathData(query::TypedValue &path_in_frame) : path_in_frame(path_in_frame) {}
    std::vector<GraphElementData<Vertex>> vertices;
    std::vector<GraphElementData<Edge>> edges;
    query::TypedValue &path_in_frame;
  };

 public:
  RemotePullResData() {}  // Default constructor required for serialization.
  RemotePullResData(int worker_id, bool send_old, bool send_new)
      : worker_id(worker_id), send_old(send_old), send_new(send_new) {}

  RemotePullResData(const RemotePullResData &) = delete;
  RemotePullResData &operator=(const RemotePullResData &) = delete;
  RemotePullResData(RemotePullResData &&) = default;
  RemotePullResData &operator=(RemotePullResData &&) = default;

  RemotePullData state_and_frames;
  // Id of the worker on which the response is created, used for serializing
  // vertices (converting local to global addresses).
  int worker_id;
  // Indicates which of (old, new) records of a graph element should be sent.
  bool send_old;
  bool send_new;

  // Temporary caches used between deserialization and post-processing
  // (transfering the ownership of this data to a RemoteCache).
  std::vector<GraphElementData<Vertex>> vertices;
  std::vector<GraphElementData<Edge>> edges;
  std::vector<PathData> paths;

  /// Saves a typed value that is a vertex/edge/path.
  template <class TArchive>
  void SaveGraphElement(TArchive &ar, const query::TypedValue &value) const {
    // Helper template function for storing a vertex or an edge.
    auto save_element = [&ar, this](auto element_accessor) {
      ar << element_accessor.GlobalAddress().raw();

      // If both old and new are null, we need to reconstruct.
      if (!(element_accessor.GetOld() || element_accessor.GetNew())) {
        bool result = element_accessor.Reconstruct();
        CHECK(result) << "Attempting to serialize an element not visible to "
                         "current transaction.";
      }
      auto *old_rec = element_accessor.GetOld();
      if (send_old && old_rec) {
        ar << true;
        distributed::SaveElement(ar, *old_rec, worker_id);
      } else {
        ar << false;
      }
      if (send_new) {
        // Must call SwitchNew as that will trigger a potentially necesary
        // Reconstruct.
        element_accessor.SwitchNew();
        auto *new_rec = element_accessor.GetNew();
        if (new_rec) {
          ar << true;
          distributed::SaveElement(ar, *new_rec, worker_id);
        } else {
          ar << false;
        }
      } else {
        ar << false;
      }
    };
    switch (value.type()) {
      case query::TypedValue::Type::Vertex:
        save_element(value.ValueVertex());
        break;
      case query::TypedValue::Type::Edge:
        save_element(value.ValueEdge());
        break;
      case query::TypedValue::Type::Path: {
        auto &path = value.ValuePath();
        ar << path.size();
        save_element(path.vertices()[0]);
        for (size_t i = 0; i < path.size(); ++i) {
          save_element(path.edges()[i]);
          save_element(path.vertices()[i + 1]);
        }
        break;
      }
      default:
        LOG(FATAL) << "Unsupported graph element type: " << value.type();
    }
  }

  /// Loads a typed value that is a vertex/edge/path. Part of the
  /// deserialization process, populates the temporary data caches which are
  /// processed later.
  template <class TArchive>
  void LoadGraphElement(TArchive &ar, query::TypedValue::Type type,
                        query::TypedValue &value) {
    auto load_edge = [](auto &ar) {
      bool exists;
      ar >> exists;
      return exists ? LoadEdge(ar) : nullptr;
    };
    auto load_vertex = [](auto &ar) {
      bool exists;
      ar >> exists;
      return exists ? LoadVertex(ar) : nullptr;
    };

    switch (type) {
      case query::TypedValue::Type::Vertex: {
        storage::VertexAddress::StorageT address;
        ar >> address;
        vertices.emplace_back(storage::VertexAddress(address), load_vertex(ar),
                              load_vertex(ar), &value);
        break;
      }
      case query::TypedValue::Type::Edge: {
        storage::VertexAddress::StorageT address;
        ar >> address;
        edges.emplace_back(storage::EdgeAddress(address), load_edge(ar),
                           load_edge(ar), &value);
        break;
      }
      case query::TypedValue::Type::Path: {
        size_t path_size;
        ar >> path_size;

        paths.emplace_back(value);
        auto &path_data = paths.back();

        storage::VertexAddress::StorageT vertex_address;
        storage::EdgeAddress::StorageT edge_address;
        ar >> vertex_address;
        path_data.vertices.emplace_back(storage::VertexAddress(vertex_address),
                                        load_vertex(ar), load_vertex(ar),
                                        nullptr);
        for (size_t i = 0; i < path_size; ++i) {
          ar >> edge_address;
          path_data.edges.emplace_back(storage::EdgeAddress(edge_address),
                                       load_edge(ar), load_edge(ar), nullptr);
          ar >> vertex_address;
          path_data.vertices.emplace_back(
              storage::VertexAddress(vertex_address), load_vertex(ar),
              load_vertex(ar), nullptr);
        }
        break;
      }
      default:
        LOG(FATAL) << "Unsupported graph element type: " << type;
    }
  }
};

class RemotePullRes : public communication::rpc::Message {
 public:
  RemotePullRes() {}
  RemotePullRes(RemotePullResData data) : data(std::move(data)) {}

  RemotePullResData data;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << boost::serialization::base_object<communication::rpc::Message>(*this);
    ar << data.state_and_frames.pull_state;
    ar << data.state_and_frames.frames.size();
    // We need to indicate how many values are in each frame.
    // Assume all the frames have an equal number of elements.
    ar << (data.state_and_frames.frames.size() == 0
               ? 0
               : data.state_and_frames.frames[0].size());
    for (const auto &frame : data.state_and_frames.frames)
      for (const auto &value : frame) {
        utils::SaveTypedValue<TArchive>(
            ar, value, [this](TArchive &ar, const query::TypedValue &value) {
              data.SaveGraphElement(ar, value);
            });
      }
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> boost::serialization::base_object<communication::rpc::Message>(*this);
    ar >> data.state_and_frames.pull_state;
    size_t frame_count;
    ar >> frame_count;
    data.state_and_frames.frames.reserve(frame_count);
    size_t frame_size;
    ar >> frame_size;
    for (size_t i = 0; i < frame_count; ++i) {
      data.state_and_frames.frames.emplace_back();
      auto &current_frame = data.state_and_frames.frames.back();
      current_frame.reserve(frame_size);
      for (size_t j = 0; j < frame_size; ++j) {
        current_frame.emplace_back();
        utils::LoadTypedValue<TArchive>(
            ar, current_frame.back(),
            [this](TArchive &ar, query::TypedValue::TypedValue::Type type,
                   query::TypedValue &value) {
              data.LoadGraphElement(ar, type, value);
            });
      }
    }
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};

using RemotePullRpc =
    communication::rpc::RequestResponse<RemotePullReq, RemotePullRes>;

// TODO make a separate RPC for the continuation of an existing pull, as an
// optimization not to have to send the full RemotePullReqData pack every
// time.

RPC_SINGLE_MEMBER_MESSAGE(TransactionCommandAdvancedReq, tx::transaction_id_t);
RPC_NO_MEMBER_MESSAGE(TransactionCommandAdvancedRes);
using TransactionCommandAdvancedRpc =
    communication::rpc::RequestResponse<TransactionCommandAdvancedReq,
                                        TransactionCommandAdvancedRes>;

}  // namespace distributed
