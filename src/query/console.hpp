//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.03.17.
//

#pragma once

#include "database/graph_db.hpp"

namespace query {

/**
 * Read Evaluate Print Loop, for interacting with a database (the database in
 * the given GraphDb). Immediately starts the user-input loop and interprets the
 * entered queries.
 */
void Repl(GraphDb &);

}  // namespace query
