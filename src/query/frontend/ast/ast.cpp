#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"

namespace query {

TypedValue Ident::Evaluate(Frame& frame, SymbolTable& symbol_table) {
  return frame[symbol_table[*this].position_];
}

}
