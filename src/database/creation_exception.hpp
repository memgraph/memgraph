//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include "utils/exceptions.hpp"

/**
 * Thrown when something (Edge or a Vertex) can not
 * be created. Typically due to database overload.
 */
class CreationException : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};
