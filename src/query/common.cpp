#include "query/common.hpp"

#include <cctype>
#include <codecvt>
#include <locale>
#include <stdexcept>

#include "query/exceptions.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace query {

int64_t ParseIntegerLiteral(const std::string &s) {
  try {
    // Not really correct since long long can have a bigger range than int64_t.
    return static_cast<int64_t>(std::stoll(s, 0, 0));
  } catch (const std::out_of_range &) {
    throw SemanticException();
  }
}

std::string ParseStringLiteral(const std::string &s) {
  // These functions is declared as lambda since its semantics is highly
  // specific for this conxtext and shouldn't be used elsewhere.
  auto EncodeEscapedUnicodeCodepointUtf32 = [](const std::string &s, int &i) {
    const int kLongUnicodeLength = 8;
    int j = i + 1;
    while (j < static_cast<int>(s.size()) - 1 &&
           j < i + kLongUnicodeLength + 1 && isxdigit(s[j])) {
      ++j;
    }
    if (j - i == kLongUnicodeLength + 1) {
      char32_t t = stoi(s.substr(i + 1, kLongUnicodeLength), 0, 16);
      i += kLongUnicodeLength;
      std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
      return converter.to_bytes(t);
    }
    throw SyntaxException(
        "Expected 8 hex digits as unicode codepoint started with \\U. "
        "Use \\u for 4 hex digits format.");
  };
  auto EncodeEscapedUnicodeCodepointUtf16 = [](const std::string &s, int &i) {
    const int kShortUnicodeLength = 4;
    int j = i + 1;
    while (j < static_cast<int>(s.size()) - 1 &&
           j < i + kShortUnicodeLength + 1 && isxdigit(s[j])) {
      ++j;
    }
    if (j - i >= kShortUnicodeLength + 1) {
      char16_t t = stoi(s.substr(i + 1, kShortUnicodeLength), 0, 16);
      if (t >= 0xD800 && t <= 0xDBFF) {
        // t is high surrogate pair. Expect one more utf16 codepoint.
        j = i + kShortUnicodeLength + 1;
        if (j >= static_cast<int>(s.size()) - 1 || s[j] != '\\') {
          throw SemanticException("Invalid utf codepoint");
        }
        ++j;
        if (j >= static_cast<int>(s.size()) - 1 ||
            (s[j] != 'u' && s[j] != 'U')) {
          throw SemanticException("Invalid utf codepoint");
        }
        ++j;
        int k = j;
        while (k < static_cast<int>(s.size()) - 1 &&
               k < j + kShortUnicodeLength && isxdigit(s[k])) {
          ++k;
        }
        if (k != j + kShortUnicodeLength) {
          throw SemanticException("Invalid utf codepoint");
        }
        char16_t surrogates[3] = {t,
                                  static_cast<char16_t>(stoi(
                                      s.substr(j, kShortUnicodeLength), 0, 16)),
                                  0};
        i += kShortUnicodeLength + 2 + kShortUnicodeLength;
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>
            converter;
        return converter.to_bytes(surrogates);
      } else {
        i += kShortUnicodeLength;
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>
            converter;
        return converter.to_bytes(t);
      }
    }
    throw SyntaxException(
        "Expected 4 hex digits as unicode codepoint started with \\u. "
        "Use \\U for 8 hex digits format.");
  };

  std::string unescaped;
  bool escape = false;

  // First and last char is quote, we don't need to look at them.
  for (int i = 1; i < static_cast<int>(s.size()) - 1; ++i) {
    if (escape) {
      switch (s[i]) {
        case '\\':
          unescaped += '\\';
          break;
        case '\'':
          unescaped += '\'';
          break;
        case '"':
          unescaped += '"';
          break;
        case 'B':
        case 'b':
          unescaped += '\b';
          break;
        case 'F':
        case 'f':
          unescaped += '\f';
          break;
        case 'N':
        case 'n':
          unescaped += '\n';
          break;
        case 'R':
        case 'r':
          unescaped += '\r';
          break;
        case 'T':
        case 't':
          unescaped += '\t';
          break;
        case 'U':
          try {
            unescaped += EncodeEscapedUnicodeCodepointUtf32(s, i);
          } catch (const std::range_error &) {
            throw SemanticException("Invalid utf codepoint");
          }
          break;
        case 'u':
          try {
            unescaped += EncodeEscapedUnicodeCodepointUtf16(s, i);
          } catch (const std::range_error &) {
            throw SemanticException("Invalid utf codepoint");
          }
          break;
        default:
          // This should never happen, except grammar changes and we don't
          // notice change in this production.
          debug_assert(false, "can't happen");
          throw std::exception();
      }
      escape = false;
    } else if (s[i] == '\\') {
      escape = true;
    } else {
      unescaped += s[i];
    }
  }
  return unescaped;
}

double ParseDoubleLiteral(const std::string &s) {
  try {
    return utils::ParseDouble(s);
  } catch (const utils::BasicException &) {
    throw SemanticException("Couldn't parse string to double");
  }
}

std::string ParseParameter(const std::string &s) {
  debug_assert(s[0] == '$', "Invalid string passed as parameter name");
  if (s[1] != '`') return s.substr(1);
  // If parameter name is escaped symbolic name then symbolic name should be
  // unescaped and leading and trailing backquote should be removed.
  debug_assert(s.size() > 3U && s.back() == '`',
               "Invalid string passed as parameter name");
  std::string out;
  for (int i = 2; i < static_cast<int>(s.size()) - 1; ++i) {
    if (s[i] == '`') {
      ++i;
    }
    out.push_back(s[i]);
  }
  return out;
}
}
