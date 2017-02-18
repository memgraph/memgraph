#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "query/util.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (p:garment {garment_id: 1}) DELETE g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(Db &db, const PlanArgsT &args, Stream &stream) override {
    DbAccessor t(db);

    indices_t indices = {{"garment_id", 0}};
    auto properties = query_properties(indices, args);

    auto &label = t.label_find_or_create("garment");

    label.index()
        .for_range(t)
        .properties_filter(t, properties)
        .for_all([&](auto va) { va.remove(); });

    stream.write_empty_fields();
    stream.write_meta("w");

    return t.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
