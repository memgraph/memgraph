#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "query/util.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (n {prop: 0}) RETURN n

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(Db &db, const PlanArgsT &args, Stream &stream) override {
    DbAccessor t(db);

    auto property_key = t.vertex_property_key("prop", args[0].key.flags());

    auto vertex_accessor = t.vertex_insert();
    vertex_accessor.set(property_key, std::move(args[0]));

    stream.write_field("n");
    stream.write_vertex_record(vertex_accessor);
    stream.write_meta("w");

    return t.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
