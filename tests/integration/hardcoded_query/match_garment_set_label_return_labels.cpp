#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g:garment {garment_id: 1234}) SET g:FF RETURN labels(g)

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    stream.write_field("labels(g)");
    for (auto vertex : db_accessor.vertices()) {
      if (vertex.has_label(db_accessor.label("garment"))) {
        auto prop = vertex.PropsAt(db_accessor.property("garment_id"));
        if (prop.type_ == TypedValue::Type::Null) continue;
        auto cmp = prop == args.at(0);
        if (cmp.type_ != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        vertex.add_label(db_accessor.label("FF"));
        auto &labels = vertex.labels();
        stream.write_record();
        stream.write_list_header(1);
        stream.write_list_header(labels.size());
        for (const GraphDb::Label &label : labels) {
          stream.write(label);
        }
        stream.chunk();
      }
    }
    stream.write_meta("rw");
    return db_accessor.transaction_.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
