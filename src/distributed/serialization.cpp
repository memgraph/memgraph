#include "distributed/serialization.hpp"

namespace {

template <class TAddress>
void SaveAddress(TAddress address,
                 distributed::capnp::Address::Builder *builder,
                 int16_t worker_id) {
  builder->setGid(address.is_local() ? address.local()->gid_ : address.gid());
  builder->setWorkerId(address.is_local() ? worker_id : address.worker_id());
}

storage::VertexAddress LoadVertexAddress(
    const distributed::capnp::Address::Reader &reader) {
  return {reader.getGid(), reader.getWorkerId()};
}

storage::EdgeAddress LoadEdgeAddress(
    const distributed::capnp::Address::Reader &reader) {
  return {reader.getGid(), reader.getWorkerId()};
}

void SaveProperties(
    const PropertyValueStore &props,
    ::capnp::List<distributed::capnp::PropertyValue>::Builder *builder) {
  int64_t i = 0;
  for (const auto &kv : props) {
    auto prop_builder = (*builder)[i];
    prop_builder.setId(kv.first.Id());
    auto value_builder = prop_builder.initValue();
    distributed::SaveCapnpTypedValue(kv.second, &value_builder);
    ++i;
  }
}

PropertyValueStore LoadProperties(
    const ::capnp::List<distributed::capnp::PropertyValue>::Reader &reader) {
  PropertyValueStore props;
  for (const auto &prop_reader : reader) {
    query::TypedValue value;
    distributed::LoadCapnpTypedValue(prop_reader.getValue(), &value);
    props.set(storage::Property(prop_reader.getId()), value);
  }
  return props;
}

}  // namespace

namespace distributed {

void SaveVertex(const Vertex &vertex, capnp::Vertex::Builder *builder,
                int16_t worker_id) {
  auto save_edges = [worker_id](const auto &edges, auto *edges_builder) {
    int64_t i = 0;
    for (const auto &edge : edges) {
      auto edge_builder = (*edges_builder)[i];
      auto vertex_addr_builder = edge_builder.initVertexAddress();
      SaveAddress(edge.vertex, &vertex_addr_builder, worker_id);
      auto edge_addr_builder = edge_builder.initEdgeAddress();
      SaveAddress(edge.edge, &edge_addr_builder, worker_id);
      edge_builder.setEdgeTypeId(edge.edge_type.Id());
      ++i;
    }
  };
  auto out_builder = builder->initOutEdges(vertex.out_.size());
  save_edges(vertex.out_, &out_builder);
  auto in_builder = builder->initInEdges(vertex.in_.size());
  save_edges(vertex.in_, &in_builder);
  auto labels_builder = builder->initLabelIds(vertex.labels_.size());
  for (size_t i = 0; i < vertex.labels_.size(); ++i) {
    labels_builder.set(i, vertex.labels_[i].Id());
  }
  auto properties_builder = builder->initProperties(vertex.properties_.size());
  SaveProperties(vertex.properties_, &properties_builder);
}

std::unique_ptr<Vertex> LoadVertex(const capnp::Vertex::Reader &reader) {
  auto vertex = std::make_unique<Vertex>();
  auto load_edges = [](const auto &edges_reader) {
    Edges edges;
    for (const auto &edge_reader : edges_reader) {
      auto vertex_address = LoadVertexAddress(edge_reader.getVertexAddress());
      auto edge_address = LoadEdgeAddress(edge_reader.getEdgeAddress());
      storage::EdgeType edge_type(edge_reader.getEdgeTypeId());
      edges.emplace(vertex_address, edge_address, edge_type);
    }
    return edges;
  };
  vertex->out_ = load_edges(reader.getOutEdges());
  vertex->in_ = load_edges(reader.getInEdges());
  for (const auto &label_id : reader.getLabelIds()) {
    vertex->labels_.emplace_back(label_id);
  }
  vertex->properties_ = LoadProperties(reader.getProperties());
  return vertex;
}

void SaveEdge(const Edge &edge, capnp::Edge::Builder *builder,
              int16_t worker_id) {
  auto from_builder = builder->initFrom();
  SaveAddress(edge.from_, &from_builder, worker_id);
  auto to_builder = builder->initTo();
  SaveAddress(edge.to_, &to_builder, worker_id);
  builder->setTypeId(edge.edge_type_.Id());
  auto properties_builder = builder->initProperties(edge.properties_.size());
  SaveProperties(edge.properties_, &properties_builder);
}

std::unique_ptr<Edge> LoadEdge(const capnp::Edge::Reader &reader) {
  auto from = LoadVertexAddress(reader.getFrom());
  auto to = LoadVertexAddress(reader.getTo());
  auto edge =
      std::make_unique<Edge>(from, to, storage::EdgeType{reader.getTypeId()});
  edge->properties_ = LoadProperties(reader.getProperties());
  return edge;
}

}  // namespace distributed
