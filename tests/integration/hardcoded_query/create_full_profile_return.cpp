#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"
#include "query/parameters.hpp"

using std::cout;
using std::endl;

// Query: CREATE (p:profile {profile_id: 112, partner_id: 55}) RETURN p

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    auto v = db_accessor.insert_vertex();
    v.PropsSet(db_accessor.property("profile_id"), args.At(0));
    v.PropsSet(db_accessor.property("partner_id"), args.At(1));
    v.add_label(db_accessor.label("profile"));
    std::vector<std::string> headers{std::string("p")};
    stream.Header(headers);
    std::vector<TypedValue> result{TypedValue(v)};
    stream.Result(result);
    std::map<std::string, TypedValue> meta{std::make_pair(std::string("type"), TypedValue(std::string("rw")))};
    stream.Summary(meta);
    db_accessor.commit();
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
