#pragma once

#include "database/graph_db_accessor.hpp"

#include "query/stripped.hpp"

/**
 * @class PlanInterface
 *
 * @brief Pure Abstract class that is interface to query plans.
 *
 * @tparam Stream stream for results writing.
 */
template <typename Stream>
class PlanInterface {
 public:
  /**
   * In derived classes this method has to be overriden in order to implement
   * concrete execution plan.
   *
   * @param db_accessor Accessor for ihe database.
   * @param args Plan arguments (including literals stripped from the query).
   * @param stream stream for results writing
   *
   * @return bool status after execution (success OR fail)
   */
  virtual bool run(GraphDbAccessor &db_accessor, const Parameters &args,
                   Stream &stream) = 0;

  /**
   * Virtual destructor in base class.
   */
  virtual ~PlanInterface() {}
};

/**
 * Defines type of underlying extern C functions and library object class name.
 * (ObjectPrototype).
 *
 * @tparam underlying object depends on Stream template parameter because
 *         it will send the results throughout a custom Stream object.
 */
template <typename Stream>
struct QueryPlanTrait {
  using ObjectPrototype = PlanInterface<Stream>;
  using ProducePrototype = PlanInterface<Stream> *(*)();
  using DestructPrototype = void (*)(PlanInterface<Stream> *);
};
