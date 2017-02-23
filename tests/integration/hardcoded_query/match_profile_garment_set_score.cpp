#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (p:profile {profile_id: 111, partner_id: 55}), (g:garment
// {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=1500 RETURN s

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    stream.write_field("r");
    std::vector<VertexAccessor> g1_set, g2_set;
    for (auto g1 : db_accessor.vertices()) {
      if (g1.has_label(db_accessor.label("profile"))) {
        auto prop = g1.PropsAt(db_accessor.property("profile_id"));
        if (prop.type_ == TypedValue::Type::Null) continue;
        auto cmp = prop == args.at(0);
        if (cmp.type_ != TypedValue::Type::Bool) continue;

        auto prop2 = g1.PropsAt(db_accessor.property("partner_id"));
        if (prop.type_ == TypedValue::Type::Null) continue;
        auto cmp2 = prop == args.at(1);
        if (cmp.type_ != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        g1_set.push_back(g1);
      }
    }
    for (auto g2 : db_accessor.vertices()) {
      if (g2.has_label(db_accessor.label("garment"))) {
        auto prop = g2.PropsAt(db_accessor.property("garment_id"));
        if (prop.type_ == TypedValue::Type::Null) continue;
        auto cmp = prop == args.at(2);
        if (cmp.type_ != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        g2_set.push_back(g2);
      }
    }
    for (auto g1 : g1_set)
      for (auto g2 : g2_set) {
        EdgeAccessor e =
            db_accessor.insert_edge(g1, g2, db_accessor.edge_type("score"));
        e.PropsSet(db_accessor.property("score"), args.at(3));
        stream.write_edge_record(e);
      }
    stream.write_meta("rw");
    return db_accessor.transaction_.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
