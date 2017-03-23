//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.03.17.
//

#pragma once

#include <list>

#include "communication/result_stream_faker.hpp"
#include "dbms/dbms.hpp"

namespace query {

/**
 * Console for interacting with a database
 * (the active database in the given DBMS).
 * Immediately starts the user-input loop
 * and interprets the entered queries.
 */
void Console(Dbms &dbms);
}
