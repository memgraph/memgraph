#include <iostream>
#include <string>

#include "query/backend/cpp/typed_value.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"
#include "query/parameters.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 4567})
// CREATE (g1)-[r:default_outfit]->(g2) RETURN r

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    stream.write_field("r");
    std::vector<VertexAccessor> g1_set, g2_set;
    for (auto g1 : db_accessor.vertices()) {
      if (g1.has_label(db_accessor.label("garment"))) {
        TypedValue prop = g1.PropsAt(db_accessor.property("garment_id"));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.At(0);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        g1_set.push_back(g1);
      }
    }
    for (auto g2 : db_accessor.vertices()) {
      if (g2.has_label(db_accessor.label("garment"))) {
        auto prop = g2.PropsAt(db_accessor.property("garment_id"));
        if (prop.type() == PropertyValue::Type::Null) continue;
        auto cmp = prop == args.At(1);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        g2_set.push_back(g2);
      }
    }
    for (auto g1 : g1_set)
      for (auto g2 : g2_set) {
        EdgeAccessor e = db_accessor.insert_edge(
            g1, g2, db_accessor.edge_type("default_outfit"));
        stream.write_edge_record(e);
      }
    stream.write_meta("rw");
    db_accessor.commit();
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
