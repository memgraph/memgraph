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
  tx::TransactionId tx_id;
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
  class type##Res : public communication::rpc::Message {                    \
   public:                                                                  \
    type##Res() {}                                                          \
    type##Res(const type *name, int worker_id)                              \
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

RPC_SINGLE_MEMBER_MESSAGE(VertexReq, TxGidPair);
RPC_SINGLE_MEMBER_MESSAGE(EdgeReq, TxGidPair);
RPC_SINGLE_MEMBER_MESSAGE(VertexCountReq, tx::TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(VertexCountRes, int64_t);

using VertexRpc = communication::rpc::RequestResponse<VertexReq, VertexRes>;
using EdgeRpc = communication::rpc::RequestResponse<EdgeReq, EdgeRes>;
using VertexCountRpc =
    communication::rpc::RequestResponse<VertexCountReq, VertexCountRes>;

}  // namespace distributed
