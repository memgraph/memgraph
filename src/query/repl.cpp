#include "repl.hpp"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>
#include <vector>

#include "communication/result_stream_faker.hpp"
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

void query::Repl(database::GraphDb *db, query::Interpreter *interpreter) {
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
      auto dba = db->Access();
      ResultStreamFaker<query::TypedValue> stream;
#ifndef MG_DISTRIBUTED
      auto results = (*interpreter)(command, dba, {}, false);
#else
      auto results = (*interpreter)(command, *dba, {}, false);
#endif
      stream.Header(results.header());
      results.PullAll(stream);
      stream.Summary(results.summary());
      std::cout << stream;
#ifndef MG_DISTRIBUTED
      dba.Commit();
#else
      dba->Commit();
#endif
    } catch (const query::SyntaxException &e) {
      std::cout << "SYNTAX EXCEPTION: " << e.what() << std::endl;
    } catch (const query::LexingException &e) {
      std::cout << "LEXING EXCEPTION: " << e.what() << std::endl;
    } catch (const query::SemanticException &e) {
      std::cout << "SEMANTIC EXCEPTION: " << e.what() << std::endl;
    } catch (const query::QueryRuntimeException &e) {
      std::cout << "RUNTIME EXCEPTION: " << e.what() << std::endl;
    } catch (const query::TypedValueException &e) {
      std::cout << "TYPED VALUE EXCEPTION: " << e.what() << std::endl;
    } catch (const query::HintedAbortError &e) {
      std::cout << "HINTED ABORT ERROR: " << e.what() << std::endl;
    } catch (const utils::NotYetImplemented &e) {
      std::cout << e.what() << std::endl;
    }
  }
}
