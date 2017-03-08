#include <iostream>
#include <string>

#include "match_garment_set_label_general_return.hpp"
#include "query/plan_interface.hpp"
#include "using.hpp"
#include "query/parameters.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g:garment {garment_id: 1234}) SET g:AA RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    return run_general_query(db_accessor, args, stream, "AA");
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
