#include <iostream>
#include <string>

#include "query/backend/cpp/typed_value.hpp"
#include "query/parameters.hpp"
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
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    std::vector<std::string> headers{std::string("r")};
    stream.Header(headers);
    std::vector<VertexAccessor> g1_set, g2_set;
    for (auto g1 : db_accessor.vertices()) {
      if (g1.has_label(db_accessor.label("profile"))) {
        auto prop = TypedValue(g1.PropsAt(db_accessor.property("profile_id")));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.At(0);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;

        auto prop2 = TypedValue(g1.PropsAt(db_accessor.property("partner_id")));
        if (prop2.type() == TypedValue::Type::Null) continue;
        auto cmp2 = prop2 == args.At(1);
        if (cmp2.type() != TypedValue::Type::Bool) continue;
        if (cmp2.Value<bool>() != true) continue;
        g1_set.push_back(g1);
      }
    }
    for (auto g2 : db_accessor.vertices()) {
      if (g2.has_label(db_accessor.label("garment"))) {
        auto prop = TypedValue(g2.PropsAt(db_accessor.property("garment_id")));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.At(2);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        g2_set.push_back(g2);
      }
    }
    for (auto g1 : g1_set)
      for (auto g2 : g2_set) {
        EdgeAccessor e =
            db_accessor.insert_edge(g1, g2, db_accessor.edge_type("score"));
        e.PropsSet(db_accessor.property("score"), args.At(3));
        std::vector<TypedValue> result{TypedValue(e)};
        stream.Result(result);
      }
    std::map<std::string, TypedValue> meta{
        std::make_pair(std::string("type"), TypedValue(std::string("rw")))};
    stream.Summary(meta);
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
