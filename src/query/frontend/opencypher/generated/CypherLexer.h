
// Generated from /home/buda/Workspace/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4 by ANTLR 4.6

#pragma once


#include "antlr4-runtime.h"


namespace antlropencypher {


class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, T__46 = 47, T__47 = 48, T__48 = 49, StringLiteral = 50, 
    EscapedChar = 51, HexInteger = 52, DecimalInteger = 53, OctalInteger = 54, 
    HexLetter = 55, HexDigit = 56, Digit = 57, NonZeroDigit = 58, NonZeroOctDigit = 59, 
    OctDigit = 60, ZeroDigit = 61, ExponentDecimalReal = 62, RegularDecimalReal = 63, 
    UNION = 64, ALL = 65, OPTIONAL = 66, MATCH = 67, UNWIND = 68, AS = 69, 
    MERGE = 70, ON = 71, CREATE = 72, SET = 73, DETACH = 74, DELETE = 75, 
    REMOVE = 76, WITH = 77, DISTINCT = 78, RETURN = 79, ORDER = 80, BY = 81, 
    L_SKIP = 82, LIMIT = 83, ASCENDING = 84, ASC = 85, DESCENDING = 86, 
    DESC = 87, WHERE = 88, OR = 89, XOR = 90, AND = 91, NOT = 92, IN = 93, 
    STARTS = 94, ENDS = 95, CONTAINS = 96, IS = 97, CYPHERNULL = 98, COUNT = 99, 
    FILTER = 100, EXTRACT = 101, ANY = 102, NONE = 103, SINGLE = 104, TRUE = 105, 
    FALSE = 106, UnescapedSymbolicName = 107, IdentifierStart = 108, IdentifierPart = 109, 
    EscapedSymbolicName = 110, SP = 111, WHITESPACE = 112, Comment = 113, 
    L_0X = 114
  };

  CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace antlropencypher
