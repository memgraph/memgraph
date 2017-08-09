#include <iostream>
#include <string>

#include "query/frontend/stripped.hpp"
#include "query/parameters.hpp"
#include "query/plan_interface.hpp"
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
    for (auto v : db_accessor.Vertices(false))
      db_accessor.DetachRemoveVertex(v);
    std::vector<std::string> headers;
    stream.Header(headers);
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
