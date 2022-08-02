
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypherLexer.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"


namespace antlropencypher {


class  MemgraphCypherLexer : public antlr4::Lexer {
public:
  enum {
    UNDERSCORE = 1, AFTER = 2, ALTER = 3, ASYNC = 4, AUTH = 5, BAD = 6, 
    BATCH_INTERVAL = 7, BATCH_LIMIT = 8, BATCH_SIZE = 9, BEFORE = 10, BOOTSTRAP_SERVERS = 11, 
    CHECK = 12, CLEAR = 13, COMMIT = 14, COMMITTED = 15, CONFIG = 16, CONFIGS = 17, 
    CONSUMER_GROUP = 18, CREDENTIALS = 19, CSV = 20, DATA = 21, DELIMITER = 22, 
    DATABASE = 23, DENY = 24, DIRECTORY = 25, DROP = 26, DUMP = 27, DURABILITY = 28, 
    EXECUTE = 29, FOR = 30, FOREACH = 31, FREE = 32, FREE_MEMORY = 33, FROM = 34, 
    GLOBAL = 35, GRANT = 36, GRANTS = 37, HEADER = 38, IDENTIFIED = 39, 
    IGNORE = 40, ISOLATION = 41, KAFKA = 42, LEVEL = 43, LOAD = 44, LOCK = 45, 
    MAIN = 46, MODE = 47, MODULE_READ = 48, MODULE_WRITE = 49, NEXT = 50, 
    NO = 51, PASSWORD = 52, PORT = 53, PRIVILEGES = 54, PULSAR = 55, READ = 56, 
    READ_FILE = 57, REGISTER = 58, REPLICA = 59, REPLICAS = 60, REPLICATION = 61, 
    REVOKE = 62, ROLE = 63, ROLES = 64, QUOTE = 65, SERVICE_URL = 66, SESSION = 67, 
    SETTING = 68, SETTINGS = 69, SNAPSHOT = 70, START = 71, STATS = 72, 
    STOP = 73, STREAM = 74, STREAMS = 75, SYNC = 76, TIMEOUT = 77, TO = 78, 
    TOPICS = 79, TRANSACTION = 80, TRANSFORM = 81, TRIGGER = 82, TRIGGERS = 83, 
    UNCOMMITTED = 84, UNLOCK = 85, UPDATE = 86, USER = 87, USERS = 88, VERSION = 89, 
    WEBSOCKET = 90, Skipped = 91, LPAREN = 92, RPAREN = 93, LBRACK = 94, 
    RBRACK = 95, LBRACE = 96, RBRACE = 97, COMMA = 98, DOT = 99, DOTS = 100, 
    COLON = 101, SEMICOLON = 102, DOLLAR = 103, PIPE = 104, EQ = 105, LT = 106, 
    GT = 107, LTE = 108, GTE = 109, NEQ1 = 110, NEQ2 = 111, SIM = 112, PLUS = 113, 
    MINUS = 114, ASTERISK = 115, SLASH = 116, PERCENT = 117, CARET = 118, 
    PLUS_EQ = 119, LeftArrowHeadPart = 120, RightArrowHeadPart = 121, DashPart = 122, 
    ALL = 123, AND = 124, ANY = 125, AS = 126, ASC = 127, ASCENDING = 128, 
    ASSERT = 129, BFS = 130, BY = 131, CALL = 132, CASE = 133, COALESCE = 134, 
    CONSTRAINT = 135, CONTAINS = 136, COUNT = 137, CREATE = 138, CYPHERNULL = 139, 
    DELETE = 140, DESC = 141, DESCENDING = 142, DETACH = 143, DISTINCT = 144, 
    ELSE = 145, END = 146, ENDS = 147, EXISTS = 148, EXPLAIN = 149, EXTRACT = 150, 
    FALSE = 151, FILTER = 152, IN = 153, INDEX = 154, INFO = 155, IS = 156, 
    KB = 157, KEY = 158, LIMIT = 159, L_SKIP = 160, MATCH = 161, MB = 162, 
    MEMORY = 163, MERGE = 164, NODE = 165, NONE = 166, NOT = 167, ON = 168, 
    OPTIONAL = 169, OR = 170, ORDER = 171, PROCEDURE = 172, PROFILE = 173, 
    QUERY = 174, REDUCE = 175, REMOVE = 176, RETURN = 177, SET = 178, SHOW = 179, 
    SINGLE = 180, STARTS = 181, STORAGE = 182, THEN = 183, TRUE = 184, UNION = 185, 
    UNIQUE = 186, UNLIMITED = 187, UNWIND = 188, WHEN = 189, WHERE = 190, 
    WITH = 191, WSHORTEST = 192, XOR = 193, YIELD = 194, StringLiteral = 195, 
    DecimalLiteral = 196, OctalLiteral = 197, HexadecimalLiteral = 198, 
    FloatingLiteral = 199, UnescapedSymbolicName = 200, EscapedSymbolicName = 201, 
    IdentifierStart = 202, IdentifierPart = 203
  };

  explicit MemgraphCypherLexer(antlr4::CharStream *input);

  ~MemgraphCypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

}  // namespace antlropencypher
