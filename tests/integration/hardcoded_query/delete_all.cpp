#include <iostream>
#include <string>

#include "query/parameters.hpp"
#include "query/plan_interface.hpp"
#include "query/stripped.hpp"
#include "query/typed_value.hpp"
#include "using.hpp"

using std::cout;
using std::endl;
using query::TypedValue;

// Query: MATCH (n) DETACH DELETE n

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    for (auto v : db_accessor.vertices()) db_accessor.detach_remove_vertex(v);
    std::vector<std::string> headers;
    stream.Header(headers);
    std::map<std::string, TypedValue> meta{
        std::make_pair(std::string("type"), TypedValue(std::string("rw")))};
    stream.Summary(meta);
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
