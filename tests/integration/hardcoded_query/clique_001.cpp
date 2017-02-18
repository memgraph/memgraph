#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "query/plan_interface.hpp"
#include "query/util.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"
#include "utils/memory/stack_allocator.hpp"

using std::cout;
using std::endl;

// Dressipi astar query of 4 clicks.

// Query: MATCH
// (a:garment)-[:default_outfit]-(b:garment)-[:default_outfit]-(c:garment)-[:default_outfit]-(d:garment)-[:default_outfit]-(a:garment)-[:default_outfit]-(c:garment),
// (b:garment)-[:default_outfit]-(d:garment) RETURN
// a.garment_id,b.garment_id,c.garment_id,d.garment_id

// TODO: figure out from the pattern in a query
constexpr size_t max_depth = 3;

// TODO: from query LIMIT 10
constexpr size_t limit = 10;

class Node {
 public:
  Node *parent = {nullptr};
  VertexPropertyType<Float> tkey;
  double cost;
  int depth = {0};
  double sum = {0.0};
  VertexAccessor vacc;

  Node(VertexAccessor vacc, double cost, VertexPropertyType<Float> const &tkey)
      : cost(cost), vacc(vacc), tkey(tkey) {}
  Node(VertexAccessor vacc, double cost, Node *parent,
       VertexPropertyType<Float> const &tkey)
      : cost(cost),
        vacc(vacc),
        parent(parent),
        depth(parent->depth + 1),
        tkey(tkey) {}

  double sum_vertex_score() {
    auto now = this;
    double sum = 0;
    do {
      sum += (now->vacc.at(tkey).get())->value();
      now = now->parent;
    } while (now != nullptr);
    this->sum = sum;
    return sum;
  }
};

bool vertex_filter_contained(DbAccessor &t, VertexAccessor &v, Node *before) {
  if (v.fill()) {
    bool found;
    do {
      found = false;
      before = before->parent;
      if (before == nullptr) {
        return true;
      }
    } while (v.in_contains(before->vacc));
  }
  return false;
}

template <typename Stream>
auto astar(VertexAccessor &va, DbAccessor &t, plan_args_t &, Stream &) {
  StackAllocator stack;
  std::vector<Node *> results;

  // TODO: variable part (extract)
  VertexPropertyType<Float> tkey = t.vertex_property_key<Float>("score");

  auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
  std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);

  Node *start = new (stack.allocate<Node>()) Node(va, 0, tkey);
  queue.push(start);

  size_t count = 0;
  do {
    auto now = queue.top();
    queue.pop();

    if (now->depth >= max_depth) {
      now->sum_vertex_score();
      results.emplace_back(now);

      count++;

      if (count >= limit) {
        // the limit was reached -> STOP the execution
        break;
      }

      // if the limit wasn't reached -> POP the next vertex
      continue;
    }

    iter::for_all(now->vacc.out(), [&](auto edge) {
      VertexAccessor va = edge.to();
      if (vertex_filter_contained(t, va, now)) {
        auto cost = 1 - va.at(tkey).get()->value();
        Node *n =
            new (stack.allocate<Node>()) Node(va, now->cost + cost, now, tkey);
        queue.push(n);
      }
    });
  } while (!queue.empty());

  stack.free();

  return results;
}

void reverse_stream_ids(Node *node, Stream &stream, VertexPropertyKey key) {
  if (node == nullptr) return;
  reverse_stream_ids(node->parent, stream, key);
  stream.write(node->vacc.at(key).template as<Int64>());
}

class PlanCPU : public PlanInterface<Stream> {
 public:
  bool run(Db &db, const PlanArgsT &args, Stream &stream) override {
    DbAccessor t(db);

    indices_t indices = {{"garment_id", 0}};
    auto properties = query_properties(indices, args);

    auto &label = t.label_find_or_create("garment");
    auto garment_id_prop_key =
        t.vertex_property_key("garment_id", args[0].key.flags());

    stream.write_fields(
        {{"a.garment_id", "b.garment_id", "c.garment_id", "d.garment_id"}});

    label.index()
        .for_range(t)
        .properties_filter(t, properties)
        .for_all([&](auto va) {
          auto results = astar(va, t, args, stream);
          std::sort(results.begin(), results.end(),
                    [](Node *a, Node *b) { return a->sum > b->sum; });
          for (auto node : results) {
            stream.write_record();
            stream.write_list_header(max_depth + 1);
            reverse_stream_ids(node, stream, garment_id_prop_key);
          }
        });

    stream.write_empty_fields();
    stream.write_meta("r");

    return t.commit();
  }

  ~PlanCPU() {}
};

extern "C" PlanInterface<Stream> *produce() { return new PlanCPU(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
