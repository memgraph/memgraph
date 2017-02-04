//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include "utils/exceptions/basic_exception.hpp"


class CreationException : public BasicException {
public:
  using BasicException::BasicException;
};



