#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (n {garment_id: 1234, garment_category_id: 1}) RETURN n;

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
