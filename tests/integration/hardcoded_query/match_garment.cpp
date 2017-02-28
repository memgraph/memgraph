#include <iostream>
#include <string>

#include "query/backend/cpp/typed_value.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g:garment {garment_id: 1234}) RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const PropertyValueStore<> &args,
           Stream &stream) {
    stream.write_field("g");
    for (auto vertex : db_accessor.vertices()) {
      if (vertex.has_label(db_accessor.label("garment"))) {
        TypedValue prop = vertex.PropsAt(db_accessor.property("garment_id"));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.at(0);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        stream.write_vertex_record(vertex);
      }
    }
    stream.write_meta("r");
    db_accessor.transaction_.commit();
    return true;
  }
  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
