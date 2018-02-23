#pragma once

#include <memory>
#include <string>

#include "communication/rpc/messages.hpp"
#include "distributed/serialization.hpp"
#include "storage/edge.hpp"
#include "storage/gid.hpp"
#include "storage/vertex.hpp"
#include "transactions/type.hpp"

namespace distributed {

struct TxGidPair {
  tx::transaction_id_t tx_id;
  gid::Gid gid;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &tx_id;
    ar &gid;
  }
};

#define MAKE_RESPONSE(type, name)                                           \
  class Remote##type##Res : public communication::rpc::Message {            \
   public:                                                                  \
    Remote##type##Res() {}                                                  \
    Remote##type##Res(const type *name, int worker_id)                      \
        : name_input_(name), worker_id_(worker_id) {}                       \
                                                                            \
    template <class TArchive>                                               \
    void save(TArchive &ar, unsigned int) const {                           \
      ar << boost::serialization::base_object<                              \
          const communication::rpc::Message>(*this);                        \
      Save##type(ar, *name_input_, worker_id_);                             \
    }                                                                       \
                                                                            \
    template <class TArchive>                                               \
    void load(TArchive &ar, unsigned int) {                                 \
      ar >> boost::serialization::base_object<communication::rpc::Message>( \
                *this);                                                     \
      auto v = Load##type(ar);                                              \
      v.swap(name_output_);                                                 \
    }                                                                       \
    BOOST_SERIALIZATION_SPLIT_MEMBER()                                      \
                                                                            \
    const type *name_input_;                                                \
    int worker_id_;                                                         \
    std::unique_ptr<type> name_output_;                                     \
  };

MAKE_RESPONSE(Vertex, vertex)
MAKE_RESPONSE(Edge, edge)

#undef MAKE_RESPONSE

RPC_SINGLE_MEMBER_MESSAGE(RemoteVertexReq, TxGidPair);
RPC_SINGLE_MEMBER_MESSAGE(RemoteEdgeReq, TxGidPair);

using RemoteVertexRpc =
    communication::rpc::RequestResponse<RemoteVertexReq, RemoteVertexRes>;
using RemoteEdgeRpc =
    communication::rpc::RequestResponse<RemoteEdgeReq, RemoteEdgeRes>;

}  // namespace distributed
