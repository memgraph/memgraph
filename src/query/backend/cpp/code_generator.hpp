#pragma once

#include <string>
#include <vector>
#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"
#include "antlr4-runtime.h"

using antlropencypher::CypherParser;

class CodeGenerator {
  void GenerateExpresssion();

 private:
  std::string code_;
};
