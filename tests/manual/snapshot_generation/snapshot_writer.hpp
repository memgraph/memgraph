#pragma once

#include <string>
#include <vector>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "durability/distributed/version.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/paths.hpp"
#include "glue/communication.hpp"
#include "query/typed_value.hpp"
#include "utils/file.hpp"

#include "graph_state.hpp"

namespace snapshot_generation {

// Snapshot layout is described in durability/version.hpp
static_assert(durability::kVersion == 6,
              "Wrong snapshot version, please update!");

class SnapshotWriter {
 public:
  SnapshotWriter(const std::string &path, int worker_id,
                 uint64_t vertex_generator_local_count = 0,
                 uint64_t edge_generator_local_count = 0)
      : worker_id_(worker_id), buffer_(path) {
    encoder_.WriteRAW(durability::kSnapshotMagic.data(),
                      durability::kSnapshotMagic.size());
    encoder_.WriteValue(durability::kVersion);
    encoder_.WriteInt(worker_id_);
    encoder_.WriteInt(vertex_generator_local_count);
    encoder_.WriteInt(edge_generator_local_count);
    encoder_.WriteInt(0);
    encoder_.WriteList(std::vector<communication::bolt::Value>{});
  }

  // reference to `buffer_` gets broken when moving, so let's just forbid moving
  SnapshotWriter(SnapshotWriter &&rhs) = delete;
  SnapshotWriter &operator=(SnapshotWriter &&rhs) = delete;

  template <typename TValue>
  void WriteList(const std::vector<TValue> &list) {
    encoder_.WriteList(
        std::vector<communication::bolt::Value>(list.begin(), list.end()));
  }

  storage::VertexAddress DefaultVertexAddress(gid::Gid gid) {
    return storage::VertexAddress(gid, gid::CreatorWorker(gid));
  }

  storage::EdgeAddress DefaultEdgeAddress(gid::Gid gid) {
    return storage::EdgeAddress(gid, gid::CreatorWorker(gid));
  }

  void WriteInlineEdge(const Edge &edge, bool write_from) {
    encoder_.WriteInt(DefaultEdgeAddress(edge.gid).raw());
    encoder_.WriteInt(write_from ? DefaultVertexAddress(edge.from).raw()
                                 : DefaultVertexAddress(edge.to).raw());
    encoder_.WriteString(edge.type);
  }

  void WriteNode(const Node &node,
                 const std::unordered_map<gid::Gid, Edge> &edges) {
    encoder_.WriteRAW(
        utils::UnderlyingCast(communication::bolt::Marker::TinyStruct) + 3);
    encoder_.WriteRAW(
        utils::UnderlyingCast(communication::bolt::Signature::Node));
    encoder_.WriteInt(node.gid);

    WriteList(node.labels);
    std::map<std::string, communication::bolt::Value> props;
    for (const auto &prop : node.props) {
      props[prop.first] = glue::ToBoltValue(prop.second);
    }
    encoder_.WriteMap(props);

    // cypher_id
    encoder_.WriteInt(utils::MemcpyCast<int64_t>(node.gid));

    encoder_.WriteInt(node.in_edges.size());
    for (const auto &edge_idx : node.in_edges) {
      WriteInlineEdge(edges.at(edge_idx), true);
    }

    encoder_.WriteInt(node.out_edges.size());
    for (const auto &edge_idx : node.out_edges) {
      WriteInlineEdge(edges.at(edge_idx), false);
    }

    ++nodes_written_;
  }

  void WriteEdge(const Edge &edge) {
    encoder_.WriteRAW(
        utils::UnderlyingCast(communication::bolt::Marker::TinyStruct) + 5);
    encoder_.WriteRAW(
        utils::UnderlyingCast(communication::bolt::Signature::Relationship));
    encoder_.WriteInt(edge.gid);
    encoder_.WriteInt(edge.from);
    encoder_.WriteInt(edge.to);
    encoder_.WriteString(edge.type);
    std::map<std::string, communication::bolt::Value> props;
    for (const auto &prop : edge.props) {
      props[prop.first] = glue::ToBoltValue(prop.second);
    }
    encoder_.WriteMap(props);

    // cypher_id
    encoder_.WriteInt(utils::MemcpyCast<int64_t>(edge.gid));

    ++edges_written_;
  }

  void Close() {
    buffer_.WriteValue(nodes_written_);
    buffer_.WriteValue(edges_written_);
    buffer_.WriteValue(buffer_.hash());
    buffer_.Close();
  }

 private:
  int worker_id_;
  int64_t nodes_written_{0};
  int64_t edges_written_{0};

  HashedFileWriter buffer_;
  communication::bolt::BaseEncoder<HashedFileWriter> encoder_{buffer_};
};

void WriteToSnapshot(GraphState &state, const std::string &path) {
  for (int worker_id = 0; worker_id < state.NumWorkers(); ++worker_id) {
    const std::experimental::filesystem::path durability_dir =
        path / std::experimental::filesystem::path("worker_" +
                                                   std::to_string(worker_id));
    if (!utils::EnsureDir(durability_dir / "snapshots")) {
      LOG(ERROR) << "Unable to create durability directory!";
      exit(0);
    }
    // Pretend like this is coming from transaction numbered 0 - this is hack
    // TODO(dgleich): Change this to something more reasonable
    const auto snapshot_file =
        durability::MakeSnapshotPath(durability_dir, worker_id, 0);

    SnapshotWriter writer(snapshot_file, worker_id,
                          state.NumNodesOnWorker(worker_id),
                          state.NumEdgesOnWorker(worker_id));

    writer.WriteList(state.Indices());

    for (const auto &node : state.NodesOnWorker(worker_id)) {
      writer.WriteNode(state.GetNode(node), state.GetEdges());
    }

    for (const auto &edge : state.EdgesOnWorker(worker_id)) {
      writer.WriteEdge(state.GetEdge(edge));
    }

    writer.Close();
  }
}

}  // namespace snapshot_generation
