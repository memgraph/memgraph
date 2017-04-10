#include <iostream>
#include <string>

#include "query/parameters.hpp"
#include "query/plan_interface.hpp"
#include "query/typed_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;
using query::TypedValue;

// Query: CREATE (g:garment {garment_id: 1234, garment_category_id:
// 1,conceals:30}) RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    auto v = db_accessor.insert_vertex();
    v.add_label(db_accessor.label("garment"));
    v.PropsSet(db_accessor.property("garment_id"), args.At(0));
    v.PropsSet(db_accessor.property("garment_category_id"), args.At(1));
    v.PropsSet(db_accessor.property("conceals"), args.At(2));
    std::vector<std::string> headers{std::string("g")};
    stream.Header(headers);
    std::vector<TypedValue> result{TypedValue(v)};
    stream.Result(result);
    std::map<std::string, TypedValue> meta{
        std::make_pair(std::string("type"), TypedValue(std::string("rw")))};
    stream.Summary(meta);
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
