//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.03.17.
//
#include "console.hpp"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>

#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

#ifdef HAS_READLINE

#include "readline/history.h"
#include "readline/readline.h"

/**
 * Helper function that reads a line from the
 * standard input using the 'readline' lib.
 * Adds support for history and reverse-search.
 *
 * @param prompt The prompt to display.
 * @return  A single command the user entered.
 *  Possibly empty.
 */
std::string ReadLine(const char *prompt) {
  char *line = readline(prompt);
  if (!line) return "";

  if (*line) add_history(line);
  std::string r_val(line);
  free(line);
  return r_val;
}

#else

std::string ReadLine(const char *prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  return line;
}

#endif  // HAS_READLINE

/**
 * Converts the given TypedValue into a string (single line).
 */
std::string TypedValueToString(const query::TypedValue &value) {
  std::stringstream ss;
  switch (value.type()) {
    case query::TypedValue::Type::List:
      break;
    case query::TypedValue::Type::Map:
      break;
    case query::TypedValue::Type::Path:
      break;
    default:
      ss << value;
  }
  return ss.str();
}

/**
 * Prints out all the given results to standard out.
 */
void PrintResults(ResultStreamFaker results) {
  const std::vector<std::string> &header = results.GetHeader();
  std::vector<int> column_widths(header.size());
  std::transform(header.begin(), header.end(), column_widths.begin(),
                 [](const auto &s) { return s.size(); });

  // convert all the results into strings, and track max column width
  auto &results_data = results.GetResults();
  std::vector<std::vector<std::string>> result_strings(
      results_data.size(), std::vector<std::string>(column_widths.size()));
  for (int row_ind = 0; row_ind < results_data.size(); ++row_ind) {
    for (int col_ind = 0; col_ind < column_widths.size(); ++col_ind) {
      std::string string_val =
          TypedValueToString(results_data[row_ind][col_ind]);
      column_widths[col_ind] =
          std::max(column_widths[col_ind], (int)string_val.size());
      result_strings[row_ind][col_ind] = string_val;
    }
  }

  // output a results table
  // first define some helper functions
  auto emit_horizontal_line = [&]() {
    std::cout << "+";
    for (auto col_width : column_widths)
      std::cout << std::string((unsigned long)col_width + 2, '-') << "+";
    std::cout << std::endl;
  };

  auto emit_result_vec = [&](const std::vector<std::string> result_vec) {
    std::cout << "| ";
    for (int col_ind = 0; col_ind < column_widths.size(); ++col_ind) {
      const std::string &res = result_vec[col_ind];
      std::cout << res << std::string(column_widths[col_ind] - res.size(), ' ');
      std::cout << " | ";
    }
    std::cout << std::endl;
  };

  // final output of results
  emit_horizontal_line();
  emit_result_vec(results.GetHeader());
  emit_horizontal_line();
  for (const auto &result_vec : result_strings) emit_result_vec(result_vec);
  emit_horizontal_line();
  std::cout << "Found " << results_data.size() << " matching results"
            << std::endl;

  // output the summary
  std::cout << "Query summary: {";
  PrintIterable(std::cout, results.GetSummary(), ", ", [&](const auto kv) {
    return kv.first + ": " + TypedValueToString(kv.second);
  });
  std::cout << "}" << std::endl;
}

void query::Repl(Dbms &dbms) {
  std::cout
      << "Welcome to *Awesome* Memgraph Read Evaluate Print Loop (AM-REPL)"
      << std::endl;
  while (true) {
    std::string command = ReadLine(">");
    if (command.size() == 0) continue;

    // special commands
    if (command == "quit") break;

    // regular cypher queries
    try {
      auto dba = dbms.active();
      ResultStreamFaker results;
      query::Interpret(command, *dba, results);
      PrintResults(results);
      dba->commit();
    } catch (const query::SyntaxException &e) {
      std::cout << "SYNTAX EXCEPTION: " << e.what() << std::endl;
    } catch (const query::SemanticException &e) {
      std::cout << "SEMANTIC EXCEPTION: " << e.what() << std::endl;
    } catch (const query::QueryRuntimeException &e) {
      std::cout << "RUNTIME EXCEPTION: " << e.what() << std::endl;
    } catch (const query::TypedValueException &e) {
      std::cout << "TYPED VALUE EXCEPTION: " << e.what() << std::endl;
    } catch (const utils::NotYetImplemented &e) {
      std::cout << e.what() << std::endl;
    }
  }
}
