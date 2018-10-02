#include <gflags/gflags.h>
#include <glog/logging.h>

#include <pwd.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <algorithm>
#include <experimental/filesystem>
#include <thread>
#include <unordered_set>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

namespace fs = std::experimental::filesystem;

DEFINE_string(host, "127.0.0.1",
              "Server address. It can be a DNS resolveable hostname.");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, true, "Set to true to connect with SSL to the server.");
DEFINE_bool(fit_to_screen, false, "Fit output width to screen width.");

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
std::string default_text;

#ifdef HAS_READLINE

#include "readline/history.h"
#include "readline/readline.h"

/** Helper function that sets default input for 'readline'*/
static int SetDefaultText() {
  rl_insert_text(default_text.c_str());
  default_text = "";
  rl_startup_hook = (rl_hook_func_t *)NULL;
  return 0;
}

/** Memgraph and OpenCypher keywords.
 *  Copied from src/query/frontend/opencypher/grammar/Cypher.g4
 *  and src/query/frontend/grammar/MemgraphCypher.g4 */
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

/**
 * Helper function that reads a line from the
 * standard input using the 'readline' lib.
 * Adds support for history and reverse-search.
 *
 * @param prompt The prompt to display.
 * @return  User input line, or nullopt on EOF.
 */
static std::experimental::optional<std::string> ReadLine(
    const std::string &prompt) {
  if (default_text.size() > 0) {
    // Initialize text with remainder of previous query.
    rl_startup_hook = SetDefaultText;
  }
  char *line = readline(prompt.c_str());
  if (!line) return std::experimental::nullopt;

  std::string r_val(line);
  if (!utils::Trim(r_val).empty()) add_history(line);
  free(line);
  return r_val;
}

#else

/** Helper function that reads a line from the standard input
 *  using getline.
 *  @param prompt The prompt to display.
 *  @return User input line, or nullopt on EOF.
 */
static std::experimental::optional<std::string> ReadLine(
    const std::string &prompt) {
  std::cout << prompt << default_text;
  std::string line;
  std::getline(std::cin, line);
  if (!isatty(STDIN_FILENO)) {
    // Stupid hack to have same output as readline if stdin is redirected.
    std::cout << line << std::endl;
  }
  line = default_text + line;
  default_text = "";
  if (std::cin.eof()) return std::experimental::nullopt;
  return line;
}

#endif  // HAS_READLINE

/** Helper function that parses user line input.
 *  @param line user input line.
 *  @param quote quote character or '\0'; if set line is inside quotation.
 *  @param escaped if set, next character should be escaped.
 *  @return pair of string and bool. string is parsed line and bool marks
 *  if query finished(Query finishes with ';') with this line.
 */
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

static std::experimental::optional<std::string> GetQuery(
    const std::string &prompt) {
  char quote = '\0';
  bool escaped = false;
  auto ret = ParseLine(default_text, &quote, &escaped);
  if (ret.second) {
    auto idx = ret.first.size() + 1;
    default_text = utils::Trim(default_text.substr(idx));
    return ret.first;
  }
  std::stringstream query;
  std::string multiline_prompt("       -> ");
  int line_cnt = 0;
  auto is_done = false;
  while (!is_done) {
    auto line = ReadLine(line_cnt == 0 ? prompt : multiline_prompt);
    if (!line) return std::experimental::nullopt;
    if (line->empty()) continue;
    auto ret = ParseLine(*line, &quote, &escaped);
    query << ret.first << " ";
    auto char_count = ret.first.size();
    if (ret.second) {
      is_done = true;
      char_count += 1;
    }
    if (char_count < line->size()) {
      default_text = utils::Trim(line->substr(char_count));
    }
    ++line_cnt;
  }
  return query.str();
}

template <typename T>
static void PrettyPrintData(const std::vector<T> &data, int total_width,
                            int column_width, int num_columns,
                            bool all_columns_fit) {
  if (!all_columns_fit) num_columns -= 1;
  std::string data_output = std::string(total_width, ' ');
  for (auto i = 0; i < total_width; i += column_width) {
    data_output[i] = '|';
    int idx = i / column_width;
    if (idx < num_columns) {
      std::stringstream field;
      field << data[idx];  // convert Value to string
      std::string field_str(field.str());
      if (field_str.size() > column_width - 1) {
        field_str.erase(column_width - 1, std::string::npos);
        field_str.replace(field_str.size() - 3, 3, "...");
      }
      data_output.replace(i + 1, field_str.size(), field_str);
    }
  }
  if (!all_columns_fit) {
    data_output.replace(total_width - column_width, 3, "...");
  }
  data_output[total_width - 1] = '|';
  std::cout << data_output << std::endl;
}

/**
 * Helper function for determining maximum length of data.
 * @param data Vector of string representable elements. Elements should have
 * operator '<<' implemented.
 * @return length needed for representing max size element in vector. Plus
 * one is added because of column start character '|'.
 */
template <typename T>
static uint64_t GetMaxColumnWidth(const std::vector<T> &data) {
  uint64_t column_width = 0;
  for (auto &elem : data) {
    std::stringstream field;
    field << elem;
    column_width = std::max(column_width, field.str().size());
  }
  return column_width + 1;
}

static void PrettyPrint(
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
  PrettyPrintData(header, total_width, column_width, num_columns,
                  all_columns_fit);
  std::cout << line_fill << std::endl;
  // Print Records.
  for (size_t i = 0; i < records.size(); ++i) {
    PrettyPrintData(records[i], total_width, column_width, num_columns,
                    all_columns_fit);
  }
  std::cout << line_fill << std::endl;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
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
    std::cout << fmt::format("History directory {} doesn't exist", history_dir)
              << std::endl;
    // Should program exit here or just continue with warning message?
    return 1;
  }
  fs::path history_file = history_dir / kHistoryFilename;
  // Read history file.
  if (fs::exists(history_file)) {
    auto ret = read_history(history_file.string().c_str());
    if (ret != 0) {
      std::cout << fmt::format("Unable to read history file: {}", history_file);
      // Should program exit here or just continue with warning message?
      return 1;
    }
    history_len = history_length;
  }
#endif

  // TODO handle endpoint exception.
  // It has CHECK in constructor if address is not valid.
  io::network::Endpoint endpoint(io::network::ResolveHostname(FLAGS_host),
                                 FLAGS_port);
  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);

  if (!client.Connect(endpoint, FLAGS_username, FLAGS_password)) {
    // Error message is logged in client.Connect method
    return 1;
  }

  std::cout << "mg-client" << std::endl;
  std::cout << fmt::format("Connected to 'memgraph://{}'", endpoint)
            << std::endl;
  int num_retries = 3;
  while (true) {
    auto query = GetQuery("memgraph> ");
    if (!query) break;
    if (query->empty()) continue;
    try {
      auto ret = client.Execute(*query, {});
      PrettyPrint(ret.fields, ret.records);
    } catch (const communication::bolt::ClientQueryException &e) {
      std::cout << "Client received exception: " << e.what() << std::endl;
    } catch (const communication::bolt::ClientFatalException &e) {
      std::cout << "Client received exception: " << e.what() << std::endl;
      std::cout << "Trying to reconnect" << std::endl;
      bool is_connected = false;
      client.Close();
      while (num_retries > 0) {
        --num_retries;
        if (client.Connect(endpoint, FLAGS_username, FLAGS_password)) {
          is_connected = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      if (is_connected) {
        num_retries = 3;
        std::cout << fmt::format("Connected to 'memgraph://{}'", endpoint)
                  << std::endl;
      } else {
        std::cout << fmt::format("Couldn't connect to 'memgraph://{}'",
                                 endpoint)
                  << std::endl;
        return 1;
      }
    }
  }

#if HAS_READLINE
  int curr_len = history_length;
  if (!FLAGS_no_history && curr_len - history_len > 0) {
    int ret = 0;
    // If there was no history, create history file.
    // Otherwise, append to existing history.
    if (history_len == 0) {
      ret = write_history(history_file.string().c_str());
    } else {
      ret =
          append_history(curr_len - history_len, history_file.string().c_str());
    }
    if (ret != 0) {
      std::cout << "Unable to save history" << std::endl;
      return 1;
    }
  }
#endif
  return 0;
}
