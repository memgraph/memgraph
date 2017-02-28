#include <algorithm>
#include <bitset>
#include <iostream>
#include <string>

#include "clique.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"
#include "utils/assert.hpp"

using std::cout;
using std::endl;

// Query: MATCH
//  (a:garment)-[:default_outfit]-(b:garment)-[:default_outfit]-(c:garment)-[:default_outfit]-(d:garment)-[:default_outfit]-(a:garment)-[:default_outfit]-(c:garment),
//  (b:garment)-[:default_outfit]-(d:garment) WHERE a.garment_id=1234 RETURN
//  a.garment_id,b.garment_id,c.garment_id,d.garment_id

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    return run_general_query(db_accessor, args, stream, CliqueQuery::FIND_ALL);
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
