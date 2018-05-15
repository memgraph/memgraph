#pragma once

#include <tuple>

#include "communication/rpc/messages.hpp"
#include "distributed/bfs_subcursor.hpp"
#include "query/plan/operator.hpp"
#include "transactions/type.hpp"
#include "utils/serialization.hpp"

namespace distributed {

template <class TElement>
struct SerializedGraphElement {
  using AddressT = storage::Address<mvcc::VersionList<TElement>>;
  using AccessorT = RecordAccessor<TElement>;

  SerializedGraphElement(AddressT global_address, TElement *old_element_input,
                         TElement *new_element_input, int worker_id)
      : global_address(global_address),
        old_element_input(old_element_input),
        old_element_output(nullptr),
        new_element_input(new_element_input),
        new_element_output(nullptr),
        worker_id(worker_id) {
    CHECK(global_address.is_remote())
        << "Only global addresses should be used with SerializedGraphElement";
  }

  SerializedGraphElement(const AccessorT &accessor)
      : SerializedGraphElement(accessor.GlobalAddress(), accessor.GetOld(),
                               accessor.GetNew(),
                               accessor.db_accessor().db().WorkerId()) {}

  SerializedGraphElement() {}

  AddressT global_address;
  TElement *old_element_input;
  std::unique_ptr<TElement> old_element_output;
  TElement *new_element_input;
  std::unique_ptr<TElement> new_element_output;
  int worker_id;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void save(TArchive &ar, unsigned int) const {
    ar << global_address;
    if (old_element_input) {
      ar << true;
      SaveElement(ar, *old_element_input, worker_id);
    } else {
      ar << false;
    }
    if (new_element_input) {
      ar << true;
      SaveElement(ar, *new_element_input, worker_id);
    } else {
      ar << false;
    }
  }

  template <class TArchive>
  void load(TArchive &ar, unsigned int) {
    ar >> global_address;
    static_assert(std::is_same<TElement, Vertex>::value ||
                      std::is_same<TElement, Edge>::value,
                  "TElement should be either Vertex or Edge");
    bool has_old;
    ar >> has_old;
    if (has_old) {
      if constexpr (std::is_same<TElement, Vertex>::value) {
        old_element_output = std::move(LoadVertex(ar));
      } else {
        old_element_output = std::move(LoadEdge(ar));
      }
    }
    bool has_new;
    ar >> has_new;
    if (has_new) {
      if constexpr (std::is_same<TElement, Vertex>::value) {
        new_element_output = std::move(LoadVertex(ar));
      } else {
        new_element_output = std::move(LoadEdge(ar));
      }
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()
};  // namespace distributed

using SerializedVertex = SerializedGraphElement<Vertex>;
using SerializedEdge = SerializedGraphElement<Edge>;

struct CreateBfsSubcursorReq : public communication::rpc::Message {
  tx::TransactionId tx_id;
  query::EdgeAtom::Direction direction;
  std::vector<storage::EdgeType> edge_types;
  query::GraphView graph_view;

  CreateBfsSubcursorReq(tx::TransactionId tx_id,
                        query::EdgeAtom::Direction direction,
                        std::vector<storage::EdgeType> edge_types,
                        query::GraphView graph_view)
      : tx_id(tx_id),
        direction(direction),
        edge_types(std::move(edge_types)),
        graph_view(graph_view) {}

 private:
  friend class boost::serialization::access;

  CreateBfsSubcursorReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &tx_id &direction &graph_view;
  }
};
RPC_SINGLE_MEMBER_MESSAGE(CreateBfsSubcursorRes, int64_t);

using CreateBfsSubcursorRpc =
    communication::rpc::RequestResponse<CreateBfsSubcursorReq,
                                        CreateBfsSubcursorRes>;

struct RegisterSubcursorsReq : public communication::rpc::Message {
  std::unordered_map<int, int64_t> subcursor_ids;

  RegisterSubcursorsReq(std::unordered_map<int, int64_t> subcursor_ids)
      : subcursor_ids(std::move(subcursor_ids)) {}

 private:
  friend class boost::serialization::access;

  RegisterSubcursorsReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &subcursor_ids;
  }
};
RPC_NO_MEMBER_MESSAGE(RegisterSubcursorsRes);

using RegisterSubcursorsRpc =
    communication::rpc::RequestResponse<RegisterSubcursorsReq,
                                        RegisterSubcursorsRes>;

RPC_SINGLE_MEMBER_MESSAGE(RemoveBfsSubcursorReq, int64_t);
RPC_NO_MEMBER_MESSAGE(RemoveBfsSubcursorRes);

using RemoveBfsSubcursorRpc =
    communication::rpc::RequestResponse<RemoveBfsSubcursorReq,
                                        RemoveBfsSubcursorRes>;

RPC_SINGLE_MEMBER_MESSAGE(ExpandLevelReq, int64_t);
RPC_SINGLE_MEMBER_MESSAGE(ExpandLevelRes, bool);

using ExpandLevelRpc =
    communication::rpc::RequestResponse<ExpandLevelReq, ExpandLevelRes>;

RPC_SINGLE_MEMBER_MESSAGE(SubcursorPullReq, int64_t);

struct SubcursorPullRes : public communication::rpc::Message {
  SubcursorPullRes(const VertexAccessor &vertex)
      : vertex(std::experimental::in_place, vertex) {}

  SubcursorPullRes() : vertex(std::experimental::nullopt) {}

  std::experimental::optional<SerializedVertex> vertex;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &vertex;
  }
};

using SubcursorPullRpc =
    communication::rpc::RequestResponse<SubcursorPullReq, SubcursorPullRes>;

struct SetSourceReq : public communication::rpc::Message {
  int64_t subcursor_id;
  storage::VertexAddress source;

  SetSourceReq(int64_t subcursor_id, storage::VertexAddress source)
      : subcursor_id(subcursor_id), source(source) {}

 private:
  friend class boost::serialization::access;

  SetSourceReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &subcursor_id &source;
  }
};
RPC_NO_MEMBER_MESSAGE(SetSourceRes);

using SetSourceRpc =
    communication::rpc::RequestResponse<SetSourceReq, SetSourceRes>;

struct ExpandToRemoteVertexReq : public communication::rpc::Message {
  int64_t subcursor_id;
  storage::EdgeAddress edge;
  storage::VertexAddress vertex;

  ExpandToRemoteVertexReq(int64_t subcursor_id, storage::EdgeAddress edge,
                          storage::VertexAddress vertex)
      : subcursor_id(subcursor_id), edge(edge), vertex(vertex) {}

 private:
  friend class boost::serialization::access;

  ExpandToRemoteVertexReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &subcursor_id &edge &vertex;
  }
};
RPC_SINGLE_MEMBER_MESSAGE(ExpandToRemoteVertexRes, bool);

using ExpandToRemoteVertexRpc =
    communication::rpc::RequestResponse<ExpandToRemoteVertexReq,
                                        ExpandToRemoteVertexRes>;

struct ReconstructPathReq : public communication::rpc::Message {
  int64_t subcursor_id;
  std::experimental::optional<storage::VertexAddress> vertex;
  std::experimental::optional<storage::EdgeAddress> edge;

  ReconstructPathReq(int64_t subcursor_id, storage::VertexAddress vertex)
      : subcursor_id(subcursor_id),
        vertex(vertex),
        edge(std::experimental::nullopt) {}

  ReconstructPathReq(int64_t subcursor_id, storage::EdgeAddress edge)
      : subcursor_id(subcursor_id),
        vertex(std::experimental::nullopt),
        edge(edge) {}

 private:
  friend class boost::serialization::access;

  ReconstructPathReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &subcursor_id &vertex &edge;
  }
};

struct ReconstructPathRes : public communication::rpc::Message {
  int64_t subcursor_id;
  std::vector<SerializedEdge> edges;
  std::experimental::optional<storage::VertexAddress> next_vertex;
  std::experimental::optional<storage::EdgeAddress> next_edge;

  ReconstructPathRes(
      const std::vector<EdgeAccessor> &edge_accessors,
      std::experimental::optional<storage::VertexAddress> next_vertex,
      std::experimental::optional<storage::EdgeAddress> next_edge)
      : next_vertex(std::move(next_vertex)), next_edge(std::move(next_edge)) {
    CHECK(!static_cast<bool>(next_vertex) || !static_cast<bool>(next_edge))
        << "At most one of `next_vertex` and `next_edge` should be set";
    for (const auto &edge : edge_accessors) {
      edges.emplace_back(edge);
    }
  }

 private:
  friend class boost::serialization::access;

  ReconstructPathRes() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &edges &next_vertex &next_edge;
  }
};

using ReconstructPathRpc =
    communication::rpc::RequestResponse<ReconstructPathReq, ReconstructPathRes>;

struct PrepareForExpandReq : public communication::rpc::Message {
  int64_t subcursor_id;
  bool clear;

  PrepareForExpandReq(int64_t subcursor_id, bool clear)
      : subcursor_id(subcursor_id), clear(clear) {}

 private:
  friend class boost::serialization::access;

  PrepareForExpandReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<communication::rpc::Message>(*this);
    ar &subcursor_id &clear;
  }
};
RPC_NO_MEMBER_MESSAGE(PrepareForExpandRes);

using PrepareForExpandRpc =
    communication::rpc::RequestResponse<PrepareForExpandReq,
                                        PrepareForExpandRes>;
}  // namespace distributed
