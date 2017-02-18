//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include "utils/exceptions/basic_exception.hpp"

/**
 * Thrown when something (Edge or a Vertex) can not
 * be created. Typically due to database overload.
 */
class CreationException : public BasicException {
 public:
  using BasicException::BasicException;
};
