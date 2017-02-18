#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "query/util.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (g:garment {garment_id: 1236, garment_category_id: 1}) RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(Db &db, const PlanArgsT &args, Stream &stream) override {
    DbAccessor t(db);

    auto garment_id = t.vertex_property_key("garment_id", args[0].key.flags());
    auto garment_category_id =
        t.vertex_property_key("garment_category_id", args[1].key.flags());

    auto va = t.vertex_insert();
    va.set(garment_id, std::move(args[0]));
    va.set(garment_category_id, std::move(args[1]));

    auto &garment = t.label_find_or_create("garment");
    va.add_label(garment);

    stream.write_field("g");
    stream.write_vertex_record(va);
    stream.write_meta("w");

    return t.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
