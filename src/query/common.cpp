#include "query/common.hpp"

#include <cctype>
#include <codecvt>
#include <locale>
#include <stdexcept>

#include "glog/logging.h"

#include "query/exceptions.hpp"
#include "utils/serialization.hpp"
#include "utils/string.hpp"

namespace query {

int64_t ParseIntegerLiteral(const std::string &s) {
  try {
    // Not really correct since long long can have a bigger range than int64_t.
    return static_cast<int64_t>(std::stoll(s, 0, 0));
  } catch (const std::out_of_range &) {
    throw SemanticException("Integer literal exceeds 64 bits.");
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
          throw SemanticException("Invalid UTF codepoint.");
        }
        ++j;
        if (j >= static_cast<int>(s.size()) - 1 ||
            (s[j] != 'u' && s[j] != 'U')) {
          throw SemanticException("Invalid UTF codepoint.");
        }
        ++j;
        int k = j;
        while (k < static_cast<int>(s.size()) - 1 &&
               k < j + kShortUnicodeLength && isxdigit(s[k])) {
          ++k;
        }
        if (k != j + kShortUnicodeLength) {
          throw SemanticException("Invalid UTF codepoint.");
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
            throw SemanticException("Invalid UTF codepoint.");
          }
          break;
        case 'u':
          try {
            unescaped += EncodeEscapedUnicodeCodepointUtf16(s, i);
          } catch (const std::range_error &) {
            throw SemanticException("Invalid UTF codepoint.");
          }
          break;
        default:
          // This should never happen, except grammar changes and we don't
          // notice change in this production.
          DLOG(FATAL) << "can't happen";
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
    throw SemanticException("Couldn't parse string to double.");
  }
}

std::string ParseParameter(const std::string &s) {
  DCHECK(s[0] == '$') << "Invalid string passed as parameter name";
  if (s[1] != '`') return s.substr(1);
  // If parameter name is escaped symbolic name then symbolic name should be
  // unescaped and leading and trailing backquote should be removed.
  DCHECK(s.size() > 3U && s.back() == '`')
      << "Invalid string passed as parameter name";
  std::string out;
  for (int i = 2; i < static_cast<int>(s.size()) - 1; ++i) {
    if (s[i] == '`') {
      ++i;
    }
    out.push_back(s[i]);
  }
  return out;
}

void ReconstructTypedValue(TypedValue &value) {
  using Type = TypedValue::Type;
  switch (value.type()) {
    case Type::Vertex:
      if (!value.ValueVertex().Reconstruct()) throw ReconstructionException();
      break;
    case Type::Edge:
      if (!value.ValueEdge().Reconstruct()) throw ReconstructionException();
      break;
    case Type::List:
      for (TypedValue &inner_value : value.Value<std::vector<TypedValue>>())
        ReconstructTypedValue(inner_value);
      break;
    case Type::Map:
      for (auto &kv : value.Value<std::map<std::string, TypedValue>>())
        ReconstructTypedValue(kv.second);
      break;
    case Type::Path:
      for (auto &vertex : value.ValuePath().vertices()) {
        if (!vertex.Reconstruct()) throw ReconstructionException();
      }
      for (auto &edge : value.ValuePath().edges()) {
        if (!edge.Reconstruct()) throw ReconstructionException();
      }
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
      break;
  }
}

namespace {

bool TypedValueCompare(const TypedValue &a, const TypedValue &b) {
  // in ordering null comes after everything else
  // at the same time Null is not less that null
  // first deal with Null < Whatever case
  if (a.IsNull()) return false;
  // now deal with NotNull < Null case
  if (b.IsNull()) return true;

  // comparisons are from this point legal only between values of
  // the  same type, or int+float combinations
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())))
    throw QueryRuntimeException(
        "Can't compare value of type {} to value of type {}.", a.type(),
        b.type());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return !a.Value<bool>() && b.Value<bool>();
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        return a.Value<int64_t>() < b.Value<double>();
      else
        return a.Value<int64_t>() < b.Value<int64_t>();
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        return a.Value<double>() < b.Value<int64_t>();
      else
        return a.Value<double>() < b.Value<double>();
    case TypedValue::Type::String:
      return a.Value<std::string>() < b.Value<std::string>();
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
      throw QueryRuntimeException(
          "Comparison is not defined for values of type {}.", a.type());
    default:
      LOG(FATAL) << "Unhandled comparison for types";
  }
}

}  // namespace

bool TypedValueVectorCompare::operator()(
    const std::vector<TypedValue> &c1,
    const std::vector<TypedValue> &c2) const {
  // ordering is invalid if there are more elements in the collections
  // then there are in the ordering_ vector
  DCHECK(c1.size() <= ordering_.size() && c2.size() <= ordering_.size())
      << "Collections contain more elements then there are orderings";

  auto c1_it = c1.begin();
  auto c2_it = c2.begin();
  auto ordering_it = ordering_.begin();
  for (; c1_it != c1.end() && c2_it != c2.end();
       c1_it++, c2_it++, ordering_it++) {
    if (TypedValueCompare(*c1_it, *c2_it)) return *ordering_it == Ordering::ASC;
    if (TypedValueCompare(*c2_it, *c1_it))
      return *ordering_it == Ordering::DESC;
  }

  // at least one collection is exhausted
  // c1 is less then c2 iff c1 reached the end but c2 didn't
  return (c1_it == c1.end()) && (c2_it != c2.end());
}

void TypedValueVectorCompare::Save(
    capnp::TypedValueVectorCompare::Builder *builder) const {
  auto ordering_builder = builder->initOrdering(ordering_.size());
  for (size_t i = 0; i < ordering_.size(); ++i) {
    ordering_builder.set(i, ordering_[i] == Ordering::ASC
                                ? capnp::Ordering::ASC
                                : capnp::Ordering::DESC);
  }
}

void TypedValueVectorCompare::Load(
    const capnp::TypedValueVectorCompare::Reader &reader) {
  std::vector<Ordering> ordering;
  ordering.reserve(reader.getOrdering().size());
  for (auto ordering_reader : reader.getOrdering()) {
    ordering.push_back(ordering_reader == capnp::Ordering::ASC
                           ? Ordering::ASC
                           : Ordering::DESC);
  }
  ordering_ = ordering;
}

template <typename TAccessor>
void SwitchAccessor(TAccessor &accessor, GraphView graph_view) {
  switch (graph_view) {
    case GraphView::NEW:
      accessor.SwitchNew();
      break;
    case GraphView::OLD:
      accessor.SwitchOld();
      break;
  }
}

template void SwitchAccessor<>(VertexAccessor &accessor, GraphView graph_view);
template void SwitchAccessor<>(EdgeAccessor &accessor, GraphView graph_view);

}  // namespace query
