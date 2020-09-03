#include <gflags/gflags.h>
#include <glog/logging.h>

#include <pwd.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#include <algorithm>
#include <filesystem>
#include <thread>
#include <unordered_set>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/algorithm.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/signals.hpp"
#include "utils/string.hpp"
#include "utils/terminate_handler.hpp"
#include "utils/timer.hpp"
#include "version.hpp"

namespace fs = std::filesystem;

volatile sig_atomic_t is_shutting_down = 0;

// Usage strings.
static const std::string kUsage =
    "Memgraph bolt client.\n"
    "The client can be run in interactive or non-interactive mode.\n";
static const std::string kInteractiveUsage =
    "In interactive mode, user can enter cypher queries and supported "
    "commands.\n\n"
    "Cypher queries can span through multiple lines and conclude with a\n"
    "semi-colon (;). Each query is executed in the database and the results\n"
    "are printed out.\n\n"
    "The following interactive commands are supported:\n\n"
    "\t:help\t Print out usage for interactive mode\n"
    "\t:quit\t Exit the shell\n";

// Supported commands.
// Maybe also add reconnect?
static const std::string kCommandQuit = ":quit";
static const std::string kCommandHelp = ":help";

// Supported formats.
static const std::string kCsvFormat = "csv";
static const std::string kTabularFormat = "tabular";

DEFINE_string(host, "127.0.0.1",
              "Server address. It can be a DNS resolvable hostname.");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, true, "Use SSL when connecting to the server.");
DEFINE_bool(fit_to_screen, false, "Fit output width to screen width.");
DEFINE_VALIDATED_string(
    output_format, "tabular",
    "Query output format. Can be csv/tabular. If output format is "
    "other than tabular `fit-to-screen` flag is ignored.",
    {
      if (value == kCsvFormat || value == kTabularFormat) {
        return true;
      }
      return false;
    });
DEFINE_VALIDATED_string(csv_delimiter, ",",
                        "Character used to separate fields.", {
                          if (value.size() != 1) {
                            return false;
                          }
                          return true;
                        });
DEFINE_string(
    csv_escapechar, "",
    "Character used to escape the quotechar(\") if csv-doublequote is false.");
DEFINE_bool(
    csv_doublequote, true,
    "Controls how instances of quotechar(\") appearing inside a field should "
    "themselves be quoted. When true, the character is doubled. When false, "
    "the escapechar is used as a prefix to the quotechar. "
    "If csv-doublequote is false 'csv-escapechar' must be set.");

static bool ValidateCsvDoubleQuote() {
  if (!FLAGS_csv_doublequote && FLAGS_csv_escapechar.size() != 1) {
    return false;
  }
  return true;
}

#ifdef HAS_READLINE
DEFINE_string(history, "~/.memgraph",
              "Use the specified directory for saving history.");
DEFINE_bool(no_history, false, "Do not save history.");

// History default directory.
static const std::string kDefaultHistoryBaseDir = "~";
static const std::string kDefaultHistoryMemgraphDir = ".memgraph";
// History filename.
static const std::string kHistoryFilename = "client_history";
#endif

DECLARE_int32(min_log_level);

// Unfinished query text from previous input.
// e.g. Previous input was MATCH(n) RETURN n; MATCH
// then default_text would be set to MATCH for next query.
static std::string default_text;

static const std::string kPrompt = "memgraph> ";
static const std::string kMultilinePrompt = "       -> ";

static void PrintHelp() { std::cout << kInteractiveUsage << std::endl; }

static void PrintValue(std::ostream &os, const std::string &value) {
  os << value;
}

static void PrintValue(std::ostream &os,
                       const communication::bolt::Value &value) {
  switch (value.type()) {
    case communication::bolt::Value::Type::String:
      os << value.ValueString();
      return;
    default:
      os << value;
      return;
  }
}

static void EchoFailure(const std::string &failure_msg,
                        const std::string &explanation) {
  if (isatty(STDIN_FILENO)) {
    std::cout << "\033[1;31m" << failure_msg << ": \033[0m";
    std::cout << explanation << std::endl;
  } else {
    std::cerr << failure_msg << ": ";
    std::cerr << explanation << std::endl;
  }
}

static void EchoInfo(const std::string &message) {
  if (isatty(STDIN_FILENO)) {
    std::cout << message << std::endl;
  }
}

static void SetStdinEcho(bool enable = true) {
  struct termios tty;
  tcgetattr(STDIN_FILENO, &tty);
  if (!enable) {
    tty.c_lflag &= ~ECHO;
  } else {
    tty.c_lflag |= ECHO;
  }
  tcsetattr(STDIN_FILENO, TCSANOW, &tty);
}

#ifdef HAS_READLINE

#include "readline/history.h"
#include "readline/readline.h"

/// Helper function that sets default input for 'readline'
static int SetDefaultText() {
  rl_insert_text(default_text.c_str());
  default_text = "";
  rl_startup_hook = (rl_hook_func_t *)NULL;
  return 0;
}

/// Memgraph and OpenCypher keywords.
/// Copied from src/query/frontend/opencypher/grammar/Cypher.g4
/// and src/query/frontend/grammar/MemgraphCypher.g4
static const std::vector<std::string> kMemgraphKeywords{
    "ALTER",    "AUTH",    "BATCH", "BATCHES", "CLEAR",     "DATA",
    "DENY",     "DROP",    "FOR",   "FROM",    "GRANT",     "IDENTIFIED",
    "INTERVAL", "K_TEST",  "KAFKA", "LOAD",    "PASSWORD",  "PRIVILEGES",
    "REVOKE",   "ROLE",    "ROLES", "SIZE",    "START",     "STOP",
    "STREAM",   "STREAMS", "TO",    "TOPIC",   "TRANSFORM", "USER",
    "USERS"};
static const std::vector<std::string> kCypherKeywords{
    "ALL",        "AND",    "ANY",    "AS",         "ASC",      "ASCENDING",
    "BFS",        "BY",     "CASE",   "CONTAINS",   "COUNT",    "CREATE",
    "CYPHERNULL", "DELETE", "DESC",   "DESCENDING", "DETACH",   "DISTINCT",
    "ELSE",       "END",    "ENDS",   "EXTRACT",    "FALSE",    "FILTER",
    "IN",         "INDEX",  "IS",     "LIMIT",      "L_SKIP",   "MATCH",
    "MERGE",      "NONE",   "NOT",    "ON",         "OPTIONAL", "OR",
    "ORDER",      "REDUCE", "REMOVE", "RETURN",     "SET",      "SHOW",
    "SINGLE",     "STARTS", "THEN",   "TRUE",       "UNION",    "UNWIND",
    "WHEN",       "WHERE",  "WITH",   "WSHORTEST",  "XOR"};

static char *CompletionGenerator(const char *text, int state) {
  // This function is called with state=0 the first time; subsequent calls
  // are with a nonzero state. state=0 can be used to perform one-time
  // initialization for this completion session.
  static std::vector<std::string> matches;
  static size_t match_index = 0;

  if (state == 0) {
    // During initialization, compute the actual matches for 'text' and
    // keep them in a static vector.
    matches.clear();
    match_index = 0;

    // Collect a vector of matches: vocabulary words that begin with text.
    std::string text_str = utils::ToUpperCase(std::string(text));
    for (auto word : kCypherKeywords) {
      if (word.size() >= text_str.size() &&
          word.compare(0, text_str.size(), text_str) == 0) {
        matches.push_back(word);
      }
    }
    for (auto word : kMemgraphKeywords) {
      if (word.size() >= text_str.size() &&
          word.compare(0, text_str.size(), text_str) == 0) {
        matches.push_back(word);
      }
    }
  }

  if (match_index >= matches.size()) {
    // We return nullptr to notify the caller no more matches are available.
    return nullptr;
  } else {
    // Return a malloc'd char* for the match. The caller frees it.
    return strdup(matches[match_index++].c_str());
  }
}

static char **Completer(const char *text, int start, int end) {
  // Don't do filename completion even if our generator finds no matches.
  rl_attempted_completion_over = 1;
  // Note: returning nullptr here will make readline use the default filename
  // completer. This note is copied from examples - I think because
  // rl_attempted_completion_over is set to 1, filename completer won't be used.
  return rl_completion_matches(text, CompletionGenerator);
}

/// Helper function that reads a line from the
/// standard input using the 'readline' lib.
/// Adds support for history and reverse-search.
///
/// @param prompt The prompt to display.
/// @return  User input line, or nullopt on EOF.
static std::optional<std::string> ReadLine(const std::string &prompt) {
  if (default_text.size() > 0) {
    // Initialize text with remainder of previous query.
    rl_startup_hook = SetDefaultText;
  }
  char *line = readline(prompt.c_str());
  if (!line) return std::nullopt;

  std::string r_val(line);
  if (!utils::Trim(r_val).empty()) add_history(line);
  free(line);
  return r_val;
}

#else

/// Helper function that reads a line from the standard input
/// using getline.
/// @param prompt The prompt to display.
/// @return User input line, or nullopt on EOF.
static std::optional<std::string> ReadLine(const std::string &prompt) {
  std::cout << prompt << default_text;
  std::string line;
  std::getline(std::cin, line);
  if (std::cin.eof()) return std::nullopt;
  line = default_text + line;
  default_text = "";
  return line;
}

#endif  // HAS_READLINE

static std::optional<std::string> GetLine() {
  std::string line;
  std::getline(std::cin, line);
  if (std::cin.eof()) return std::nullopt;
  line = default_text + line;
  default_text = "";
  return line;
}

/// Helper function that parses user line input.
/// @param line user input line.
/// @param quote quote character or '\0'; if set line is inside quotation.
/// @param escaped if set, next character should be escaped.
/// @return pair of string and bool. string is parsed line and bool marks
/// if query finished(Query finishes with ';') with this line.
static std::pair<std::string, bool> ParseLine(const std::string &line,
                                              char *quote, bool *escaped) {
  // Parse line.
  bool is_done = false;
  std::stringstream parsed_line;
  for (auto c : line) {
    if (*quote && c == '\\') {
      // Escaping is only used inside quotation to not end the quote
      // when quotation char is escaped.
      *escaped = !*escaped;
      parsed_line << c;
      continue;
    } else if ((!*quote && (c == '\"' || c == '\'')) ||
               (!*escaped && c == *quote)) {
      *quote = *quote ? '\0' : c;
    } else if (!*quote && c == ';') {
      is_done = true;
      break;
    }
    parsed_line << c;
    *escaped = false;
  }
  return std::make_pair(parsed_line.str(), is_done);
}

static std::optional<std::string> GetQuery() {
  char quote = '\0';
  bool escaped = false;
  auto ret = ParseLine(default_text, &quote, &escaped);
  if (ret.second) {
    auto idx = ret.first.size() + 1;
    default_text = utils::Trim(default_text.substr(idx));
    return ret.first;
  }
  std::stringstream query;
  std::optional<std::string> line;
  int line_cnt = 0;
  auto is_done = false;
  while (!is_done) {
    if (!isatty(STDIN_FILENO)) {
      line = GetLine();
    } else {
      line = ReadLine(line_cnt == 0 ? kPrompt : kMultilinePrompt);
      if (line_cnt == 0 && line && line->size() > 0 && (*line)[0] == ':') {
        auto trimmed_line = utils::Trim(*line);
        if (trimmed_line == kCommandQuit) {
          return std::nullopt;
        } else if (trimmed_line == kCommandHelp) {
          PrintHelp();
          return "";
        } else {
          EchoFailure("Unsupported command", std::string(trimmed_line));
          PrintHelp();
          return "";
        }
      }
    }
    if (!line) return std::nullopt;
    if (line->empty()) continue;
    auto ret = ParseLine(*line, &quote, &escaped);
    query << ret.first;
    auto char_count = ret.first.size();
    if (ret.second) {
      is_done = true;
      char_count += 1;  // ';' sign
    } else {
      // Query is multiline so append newline.
      query << "\n";
    }
    if (char_count < line->size()) {
      default_text = utils::Trim(line->substr(char_count));
    }
    ++line_cnt;
  }
  return query.str();
}

template <typename T>
static void PrintRowTabular(const std::vector<T> &data, int total_width,
                            int column_width, int num_columns,
                            bool all_columns_fit, int margin = 1) {
  if (!all_columns_fit) num_columns -= 1;
  std::string data_output = std::string(total_width, ' ');
  for (auto i = 0; i < total_width; i += column_width) {
    data_output[i] = '|';
    int idx = i / column_width;
    if (idx < num_columns) {
      std::stringstream field;
      PrintValue(field, data[idx]);  // convert Value to string
      std::string field_str(field.str());
      if (field_str.size() > column_width - 2 * margin - 1) {
        field_str.erase(column_width - 2 * margin - 1, std::string::npos);
        field_str.replace(field_str.size() - 3, 3, "...");
      }
      data_output.replace(i + 1 + margin, field_str.size(), field_str);
    }
  }
  if (!all_columns_fit) {
    data_output.replace(total_width - column_width, 3, "...");
  }
  data_output[total_width - 1] = '|';
  std::cout << data_output << std::endl;
}

/// Helper function for determining maximum length of data.
/// @param data Vector of string representable elements. Elements should have
/// operator '<<' implemented.
/// @param margin Column margin width.
/// @return length needed for representing max size element in vector. Plus
/// one is added because of column start character '|'.
template <typename T>
static uint64_t GetMaxColumnWidth(const std::vector<T> &data, int margin = 1) {
  uint64_t column_width = 0;
  for (auto &elem : data) {
    std::stringstream field;
    PrintValue(field, elem);
    column_width = std::max(column_width, field.str().size() + 2 * margin);
  }
  return column_width + 1;
}

static void PrintTabular(
    const std::vector<std::string> &header,
    const std::vector<std::vector<communication::bolt::Value>> &records) {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
  bool all_columns_fit = true;

  auto num_columns = header.size();
  auto column_width = GetMaxColumnWidth(header);
  for (size_t i = 0; i < records.size(); ++i) {
    column_width = std::max(column_width, GetMaxColumnWidth(records[i]));
  }
  column_width = std::max(static_cast<uint64_t>(5),
                          column_width);  // set column width to min 5
  auto total_width = column_width * num_columns + 1;

  // Fit to screen width.
  if (FLAGS_fit_to_screen && total_width > w.ws_col) {
    uint64_t lo = 5;
    uint64_t hi = column_width;
    uint64_t last = 5;
    while (lo < hi) {
      uint64_t mid = lo + (hi - lo) / 2;
      uint64_t width = mid * num_columns + 1;
      if (width <= w.ws_col) {
        last = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    column_width = last;
    total_width = column_width * num_columns + 1;
    // All columns do not fit on screen.
    while (total_width > w.ws_col && num_columns > 1) {
      num_columns -= 1;
      total_width = column_width * num_columns + 1;
      all_columns_fit = false;
    }
  }

  auto line_fill = std::string(total_width, '-');
  for (auto i = 0; i < total_width; i += column_width) {
    line_fill[i] = '+';
  }
  line_fill[total_width - 1] = '+';
  std::cout << line_fill << std::endl;
  // Print Header.
  PrintRowTabular(header, total_width, column_width, num_columns,
                  all_columns_fit);
  std::cout << line_fill << std::endl;
  // Print Records.
  for (size_t i = 0; i < records.size(); ++i) {
    PrintRowTabular(records[i], total_width, column_width, num_columns,
                    all_columns_fit);
  }
  std::cout << line_fill << std::endl;
}

template <typename T>
static std::vector<std::string> FormatCsvFields(const std::vector<T> &fields) {
  std::vector<std::string> formatted;
  formatted.reserve(fields.size());
  for (auto &field : fields) {
    std::stringstream field_stream;
    field_stream << field;
    std::string formatted_field(field_stream.str());
    if (FLAGS_csv_doublequote) {
      formatted_field = utils::Replace(formatted_field, "\"", "\"\"");
    } else {
      formatted_field =
          utils::Replace(formatted_field, "\"", FLAGS_csv_escapechar + "\"");
    }
    formatted_field.insert(0, 1, '"');
    formatted_field.append(1, '"');
    formatted.push_back(formatted_field);
  }
  return formatted;
}

static void PrintCsv(
    const std::vector<std::string> &header,
    const std::vector<std::vector<communication::bolt::Value>> &records) {
  // Print Header.
  auto formatted_header = FormatCsvFields(header);
  utils::PrintIterable(std::cout, formatted_header, FLAGS_csv_delimiter);
  std::cout << std::endl;
  // Print Records.
  for (size_t i = 0; i < records.size(); ++i) {
    auto formatted_row = FormatCsvFields(records[i]);
    utils::PrintIterable(std::cout, formatted_row, FLAGS_csv_delimiter);
    std::cout << std::endl;
  }
}

static void Output(
    const std::vector<std::string> &header,
    const std::vector<std::vector<communication::bolt::Value>> &records) {
  if (FLAGS_output_format == kTabularFormat) {
    PrintTabular(header, records);
  } else if (FLAGS_output_format == kCsvFormat) {
    PrintCsv(header, records);
  }
}

int main(int argc, char **argv) {
  gflags::SetVersionString(version_string);
  gflags::SetUsageMessage(kUsage);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_output_format == kCsvFormat && !ValidateCsvDoubleQuote()) {
    EchoFailure(
        "Unsupported combination of 'csv-doublequote' and 'csv-escapechar'\n"
        "flags",
        "Run './mg_client --help' for usage.");
    return 1;
  }
  auto password = FLAGS_password;
  if (isatty(STDIN_FILENO) && FLAGS_username.size() > 0 &&
      password.size() == 0) {
    SetStdinEcho(false);
    auto password_optional = ReadLine("Password: ");
    std::cout << std::endl;
    if (password_optional) {
      password = *password_optional;
    } else {
      EchoFailure(
          "Password not submitted",
          fmt::format("Requested password for username {}", FLAGS_username));
      return 1;
    }
    SetStdinEcho(true);
  }
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);

  communication::Init();

#ifdef HAS_READLINE
  using_history();
  int history_len = 0;
  rl_attempted_completion_function = Completer;
  fs::path history_dir = FLAGS_history;
  if (FLAGS_history ==
      (kDefaultHistoryBaseDir + "/" + kDefaultHistoryMemgraphDir)) {
    // Fetch home dir for user.
    struct passwd *pw = getpwuid(getuid());
    history_dir = fs::path(pw->pw_dir) / kDefaultHistoryMemgraphDir;
  }
  if (!utils::EnsureDir(history_dir)) {
    EchoFailure("History directory doesn't exist", history_dir);
    // Should program exit here or just continue with warning message?
    return 1;
  }
  fs::path history_file = history_dir / kHistoryFilename;
  // Read history file.
  if (fs::exists(history_file)) {
    auto ret = read_history(history_file.string().c_str());
    if (ret != 0) {
      EchoFailure("Unable to read history file", history_file);
      // Should program exit here or just continue with warning message?
      return 1;
    }
    history_len = history_length;
  }

  // Save history function. Used to save readline history after each query.
  auto save_history = [&history_len, history_file] {
    if (!FLAGS_no_history) {
      int ret = 0;
      // If there was no history, create history file.
      // Otherwise, append to existing history.
      if (history_len == 0) {
        ret = write_history(history_file.string().c_str());
      } else {
        ret = append_history(1, history_file.string().c_str());
      }
      if (ret != 0) {
        EchoFailure("Unable to save history to file", history_file);
        return 1;
      }
      ++history_len;
    }
    return 0;
  };
#endif

  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  auto shutdown = [](int exit_code = 0) {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    std::quick_exit(exit_code);
  };

  utils::SignalHandler::RegisterHandler(utils::Signal::Terminate, shutdown,
                                        block_shutdown_signals);
  utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                        block_shutdown_signals);

  // TODO handle endpoint exception.
  // It has CHECK in constructor if address is not valid.
  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_host),
                                 FLAGS_port);
  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  std::string bolt_client_version =
      fmt::format("mg_client/{}", gflags::VersionString());
  try {
    client.Connect(endpoint, FLAGS_username, password, bolt_client_version);
  } catch (const communication::bolt::ClientFatalException &e) {
    EchoFailure("Connection failure", e.what());
    return 1;
  }

  EchoInfo(fmt::format("mg_client {}", gflags::VersionString()));
  EchoInfo("Type :help for shell usage");
  EchoInfo("Quit the shell by typing Ctrl-D(eof) or :quit");
  EchoInfo(fmt::format("Connected to 'memgraph://{}'", endpoint));
  int num_retries = 3;
  while (true) {
    auto query = GetQuery();
    if (!query) {
      EchoInfo("Bye");
      break;
    }
    if (query->empty()) continue;
    try {
      utils::Timer t;
      auto ret = client.Execute(*query, {});
      auto elapsed = t.Elapsed().count();
      if (ret.records.size() > 0) Output(ret.fields, ret.records);
      if (isatty(STDIN_FILENO)) {
        std::string summary;
        if (ret.records.size() == 0) {
          summary = "Empty set";
        } else if (ret.records.size() == 1) {
          summary = std::to_string(ret.records.size()) + " row in set";
        } else {
          summary = std::to_string(ret.records.size()) + " rows in set";
        }
        std::cout << summary << " (" << fmt::format("{:.3f}", elapsed)
                  << " sec)" << std::endl;
#ifdef HAS_READLINE
        auto history_ret = save_history();
        if (history_ret != 0) return history_ret;
#endif
      }
    } catch (const communication::bolt::ClientQueryException &e) {
      if (!isatty(STDIN_FILENO)) {
        EchoFailure("Failed query", *query);
      }
      EchoFailure("Client received exception", e.what());
      if (!isatty(STDIN_FILENO)) {
        return 1;
      }
    } catch (const communication::bolt::ClientFatalException &e) {
      EchoFailure("Client received exception", e.what());
      EchoInfo("Trying to reconnect");
      bool is_connected = false;
      client.Close();
      while (num_retries > 0) {
        --num_retries;
        try {
          client.Connect(endpoint, FLAGS_username, FLAGS_password,
                         bolt_client_version);
          is_connected = true;
          break;
        } catch (const communication::bolt::ClientFatalException &e) {
          EchoFailure("Connection failure", e.what());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      if (is_connected) {
        num_retries = 3;
        EchoInfo(fmt::format("Connected to 'memgraph://{}'", endpoint));
      } else {
        EchoFailure("Couldn't connect to",
                    fmt::format("'memgraph://{}'", endpoint));
        return 1;
      }
    }
  }
  return 0;
}
