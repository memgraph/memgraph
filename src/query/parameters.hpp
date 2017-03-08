//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#ifndef MEMGRAPH_PARAMETERS_HPP
#define MEMGRAPH_PARAMETERS_HPP

#include <query/backend/cpp/typed_value.hpp>

/**
 * Encapsulates user provided parameters (and stripped literals)
 * and provides ways of obtaining them by name or position.
 */
struct Parameters {
 public:
  /**
   * Adds a value to the stripped arguments under a sequentially
   * generated name and returns a reference to that name.
   *
   * @param value
   * @return
   */
  const std::string &Add(const TypedValue &value) {
    return storage_.emplace(NextName(), value).first->first;
  }

  /**
   *  Returns the value found for the given name.
   *  The name MUST be present in this container
   *  (this is asserted).
   *
   *  @param name Param name.
   *  @return Value for the given param.
   */
  const TypedValue &At(const std::string &name) const {
    auto found = storage_.find(name);
    permanent_assert(found != storage_.end(),
                     "Name must be present in stripped arg container");
    return found->second;
  }

  /**
   * Returns the position-th stripped value. Asserts that this
   * container has at least (position + 1) elements.
   *
   * This is future proofing for when both query params and
   * stripping will be supported and naming collisions will have to
   * be avoided.
   *
   * @param position Which stripped param is sought.
   * @return Stripped param.
   */
  const TypedValue &At(const size_t position) const {
    permanent_assert(position < storage_.size(), "Invalid position");
    return storage_.find(NameForPosition(position))->second;
  }

  /** Returns the number of arguments in this container */
  const size_t Size() const { return storage_.size(); }

 private:
  std::map<std::string, TypedValue> storage_;

  /** Generates and returns a new name */
  std::string NextName() const { return NameForPosition(storage_.size()); }

  /** Returns a name for positon */
  std::string NameForPosition(unsigned long position) const {
    return "stripped_arg_" + std::to_string(position);
  }
};

#endif //MEMGRAPH_PARAMETERS_HPP
