#pragma once

#include "cereal/archives/binary.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/vector.hpp"

#include "communication/rpc/rpc.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"

#define NO_MEMBER_MESSAGE(name)                      \
  namespace tx {                                     \
  using communication::messaging::Message;           \
  struct name : public Message {                     \
    name() {}                                        \
    template <class Archive>                         \
    void serialize(Archive &ar) {                    \
      ar(cereal::virtual_base_class<Message>(this)); \
    }                                                \
  };                                                 \
  }                                                  \
  CEREAL_REGISTER_TYPE(tx::name);

#define SINGLE_MEMBER_MESSAGE(name, type)                    \
  namespace tx {                                             \
  using communication::messaging::Message;                   \
  struct name : public Message {                             \
    name() {}                                                \
    name(const type &member) : member(member) {}             \
    type member;                                             \
    template <class Archive>                                 \
    void serialize(Archive &ar) {                            \
      ar(cereal::virtual_base_class<Message>(this), member); \
    }                                                        \
  };                                                         \
  }                                                          \
  CEREAL_REGISTER_TYPE(tx::name);

SINGLE_MEMBER_MESSAGE(SnapshotReq, transaction_id_t)
SINGLE_MEMBER_MESSAGE(SnapshotRes, Snapshot)
NO_MEMBER_MESSAGE(GcSnapshotReq)
SINGLE_MEMBER_MESSAGE(ClogInfoReq, transaction_id_t)
SINGLE_MEMBER_MESSAGE(ClogInfoRes, CommitLog::Info)
SINGLE_MEMBER_MESSAGE(ActiveTransactionsReq, transaction_id_t)
SINGLE_MEMBER_MESSAGE(IsActiveReq, transaction_id_t)
SINGLE_MEMBER_MESSAGE(IsActiveRes, bool)

#undef SINGLE_MEMBER_MESSAGE
#undef NO_MEMBER_MESSAGE

namespace tx {
using SnapshotRpc =
    communication::rpc::RequestResponse<SnapshotReq, SnapshotRes>;
using GcSnapshotRpc =
    communication::rpc::RequestResponse<GcSnapshotReq, SnapshotRes>;
using GcSnapshotRpc =
    communication::rpc::RequestResponse<GcSnapshotReq, SnapshotRes>;
using ClogInfoRpc =
    communication::rpc::RequestResponse<ClogInfoReq, ClogInfoRes>;
using ActiveTransactionsRpc =
    communication::rpc::RequestResponse<ActiveTransactionsReq, SnapshotRes>;
using IsActiveRpc =
    communication::rpc::RequestResponse<IsActiveReq, IsActiveRes>;
}
