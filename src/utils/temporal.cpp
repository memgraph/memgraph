#include "utils/temporal.hpp"

#include <charconv>
#include <chrono>
#include <ctime>
#include <limits>
#include <string_view>

#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace utils {
namespace {

constexpr bool IsInBounds(const auto low, const auto high, const auto value) { return low <= value && value <= high; }

constexpr bool IsValidDay(const uint8_t day, const uint8_t month, const uint16_t year) {
  return std::chrono::year_month_day(std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}).ok();
}

template <typename T>
std::optional<T> ParseNumber(const std::string_view string, const size_t size) {
  if (string.size() < size) {
    return std::nullopt;
  }

  T value{};
  if (const auto [p, ec] = std::from_chars(string.data(), string.data() + size, value);
      ec != std::errc() || p != string.data() + size) {
    return std::nullopt;
  }

  return value;
}

}  // namespace

Date::Date(const int64_t microseconds) {
  namespace chrono = std::chrono;
  const auto chrono_micros = chrono::microseconds(microseconds);
  const auto s_days = chrono::sys_days(chrono::duration_cast<chrono::days>(chrono_micros));
  const auto date = chrono::year_month_day(s_days);
  years = static_cast<int>(date.year());
  months = static_cast<unsigned>(date.month());
  days = static_cast<unsigned>(date.day());
}

Date::Date(const DateParameters &date_parameters) {
  if (!IsInBounds(0, 9999, date_parameters.years)) {
    throw temporal::InvalidArgumentException(
        "Creating a Date with invalid year parameter. The value should be an integer between 0 and 9999.");
  }

  if (!IsInBounds(1, 12, date_parameters.months)) {
    throw temporal::InvalidArgumentException(
        "Creating a Date with invalid month parameter. The value should be an integer between 1 and 12.");
  }

  if (!IsInBounds(1, 31, date_parameters.days) ||
      !IsValidDay(date_parameters.days, date_parameters.months, date_parameters.years)) {
    throw temporal::InvalidArgumentException(
        "Creating a Date with invalid day parameter. The value should be an integer between 1 and 31, depending on the "
        "month and year.");
  }

  years = date_parameters.years;
  months = date_parameters.months;
  days = date_parameters.days;
}

namespace {
tm GetUtcFromSystemClockOrThrow() {
  namespace chrono = std::chrono;
  const auto today = chrono::system_clock::to_time_t(chrono::system_clock::now());
  tm utc_today;
  if (!gmtime_r(&today, &utc_today)) {
    throw temporal::InvalidArgumentException("Can't access clock's UTC time");
  }
  return utc_today;
}

int64_t TMYearToUtcYear(int year) { return year + 1900; }

int64_t TMMonthToUtcMonth(int month) { return month + 1; }
}  // namespace

Date UtcToday() {
  const auto utc_today = GetUtcFromSystemClockOrThrow();
  return Date({TMYearToUtcYear(utc_today.tm_year), TMMonthToUtcMonth(utc_today.tm_mon), utc_today.tm_mday});
}

LocalTime UtcLocalTime() {
  const auto utc_today = GetUtcFromSystemClockOrThrow();
  return LocalTime({utc_today.tm_hour, utc_today.tm_min, utc_today.tm_sec});
}

LocalDateTime UtcLocalDateTime() {
  const auto utc_today = GetUtcFromSystemClockOrThrow();
  return LocalDateTime({TMYearToUtcYear(utc_today.tm_year), TMMonthToUtcMonth(utc_today.tm_mon), utc_today.tm_mday},
                       {utc_today.tm_hour, utc_today.tm_min, utc_today.tm_sec});
}

namespace {
constexpr auto *kSupportedDateFormatsHelpMessage = R"help(
String representing the date should be in one of the following formats:

- YYYY-MM-DD
- YYYYMMDD
- YYYY-MM

Symbol table:
|---|-------|
| Y | YEAR  |
|---|-------|
| M | MONTH |
|---|-------|
| D | DAY   |
|---|-------|)help";

}  // namespace

std::pair<DateParameters, bool> ParseDateParameters(std::string_view date_string) {
  // https://en.wikipedia.org/wiki/ISO_8601#Dates
  // Date string with the '-' as separator are in the EXTENDED format,
  // otherwise they are in a BASIC format
  constexpr std::array valid_sizes{
      10,  // YYYY-MM-DD
      8,   // YYYYMMDD
      7    // YYYY-MM
  };

  if (!std::any_of(
          valid_sizes.begin(), valid_sizes.end(),
          [date_string_size = date_string.size()](const auto valid_size) { return valid_size == date_string_size; })) {
    throw temporal::InvalidArgumentException("Invalid string for date. {}", kSupportedDateFormatsHelpMessage);
  }

  DateParameters date_parameters;
  auto maybe_year = ParseNumber<int64_t>(date_string, 4);
  if (!maybe_year) {
    throw temporal::InvalidArgumentException("Invalid year in the string. {}", kSupportedDateFormatsHelpMessage);
  }
  date_parameters.years = *maybe_year;
  date_string.remove_prefix(4);

  bool is_extended_format = false;
  if (date_string.front() == '-') {
    is_extended_format = true;
    date_string.remove_prefix(1);
  }

  auto maybe_month = ParseNumber<int64_t>(date_string, 2);
  if (!maybe_month) {
    throw temporal::InvalidArgumentException("Invalid month in the string. {}", kSupportedDateFormatsHelpMessage);
  }
  date_parameters.months = *maybe_month;
  date_string.remove_prefix(2);

  if (!date_string.empty()) {
    if (date_string.front() == '-') {
      if (!is_extended_format) {
        throw temporal::InvalidArgumentException("Invalid format for the date. {}", kSupportedDateFormatsHelpMessage);
      }
      date_string.remove_prefix(1);
    }

    auto maybe_day = ParseNumber<int64_t>(date_string, 2);
    if (!maybe_day) {
      throw temporal::InvalidArgumentException("Invalid month in the string. {}", kSupportedDateFormatsHelpMessage);
    }
    date_parameters.days = *maybe_day;
    date_string.remove_prefix(2);
  }

  if (!date_string.empty()) {
    throw temporal::InvalidArgumentException("Invalid format for the date. {}", kSupportedDateFormatsHelpMessage);
  }

  return {date_parameters, is_extended_format};
}

int64_t Date::MicrosecondsSinceEpoch() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::microseconds>(utils::DaysSinceEpoch(years, months, days)).count();
}

int64_t Date::DaysSinceEpoch() const { return utils::DaysSinceEpoch(years, months, days).count(); }

size_t DateHash::operator()(const Date &date) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, date.years);
  result = hasher(result, date.months);
  result = hasher(result, date.days);
  return result;
}

namespace {
constexpr auto *kSupportedTimeFormatsHelpMessage = R"help(
String representing the time should be in one of the following formats:

- [T]hh:mm:ss
- [T]hh:mm

or

- [T]hhmmss
- [T]hhmm
- [T]hh

Symbol table:
|---|---------|
| h | HOURS   |
|---|---------|
| m | MINUTES |
|---|---------|
| s | SECONDS |
|---|---------|

Additionally, seconds can be defined as decimal fractions with 3 or 6 digits after the decimal point.
First 3 digits represent milliseconds, while the second 3 digits represent microseconds.)help";

}  // namespace

std::pair<LocalTimeParameters, bool> ParseLocalTimeParameters(std::string_view local_time_string) {
  // https://en.wikipedia.org/wiki/ISO_8601#Times
  // supported formats:
  //  hh:mm:ss.ssssss | hhmmss.ssssss
  //  hh:mm:ss.sss    | hhmmss.sss
  //  hh:mm           | hhmm
  //  hh
  // Times without the separator are in BASIC format
  // Times with ':' as a separator are in EXTENDED format.
  if (local_time_string.front() == 'T') {
    local_time_string.remove_prefix(1);
  }

  std::optional<bool> using_colon;
  const auto process_optional_colon = [&] {
    const bool has_colon = local_time_string.front() == ':';
    if (!using_colon.has_value()) {
      using_colon.emplace(has_colon);
    }

    if (*using_colon ^ has_colon) {
      throw temporal::InvalidArgumentException(
          "Invalid format for the local time. A separator should be used consistently or not at all. {}",
          kSupportedTimeFormatsHelpMessage);
    }

    if (has_colon) {
      local_time_string.remove_prefix(1);
    }

    if (local_time_string.empty()) {
      throw temporal::InvalidArgumentException("Invalid format for the local time. {}",
                                               kSupportedTimeFormatsHelpMessage);
    }
  };

  LocalTimeParameters local_time_parameters;

  const auto maybe_hour = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_hour) {
    throw temporal::InvalidArgumentException("Invalid hour in the string. {}", kSupportedTimeFormatsHelpMessage);
  }
  local_time_parameters.hours = *maybe_hour;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return {local_time_parameters, false};
  }

  process_optional_colon();

  const auto maybe_minute = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_minute) {
    throw temporal::InvalidArgumentException("Invalid minutes in the string. {}", kSupportedTimeFormatsHelpMessage);
  }
  local_time_parameters.minutes = *maybe_minute;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return {local_time_parameters, *using_colon};
  }

  process_optional_colon();

  const auto maybe_seconds = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_seconds) {
    throw temporal::InvalidArgumentException("Invalid seconds in the string. {}", kSupportedTimeFormatsHelpMessage);
  }
  local_time_parameters.seconds = *maybe_seconds;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return {local_time_parameters, *using_colon};
  }

  if (local_time_string.front() != '.') {
    throw temporal::InvalidArgumentException("Invalid format for local time. {}", kSupportedTimeFormatsHelpMessage);
  }
  local_time_string.remove_prefix(1);

  const auto maybe_milliseconds = ParseNumber<int64_t>(local_time_string, 3);
  if (!maybe_milliseconds) {
    throw temporal::InvalidArgumentException("Invalid milliseconds in the string. {}",
                                             kSupportedTimeFormatsHelpMessage);
  }
  local_time_parameters.milliseconds = *maybe_milliseconds;
  local_time_string.remove_prefix(3);

  if (local_time_string.empty()) {
    return {local_time_parameters, *using_colon};
  }

  const auto maybe_microseconds = ParseNumber<int64_t>(local_time_string, 3);
  if (!maybe_microseconds) {
    throw temporal::InvalidArgumentException("Invalid microseconds in the string. {}",
                                             kSupportedTimeFormatsHelpMessage);
  }
  local_time_parameters.microseconds = *maybe_microseconds;
  local_time_string.remove_prefix(3);

  if (!local_time_string.empty()) {
    throw temporal::InvalidArgumentException("Extra characters present at the end of the string.");
  }

  return {local_time_parameters, *using_colon};
}

LocalTime::LocalTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  if (chrono_microseconds.count() < 0) {
    throw temporal::InvalidArgumentException("Negative LocalTime specified in microseconds");
  }

  const auto parsed_hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
  if (parsed_hours > 23) {
    throw temporal::InvalidArgumentException("Invalid LocalTime specified in microseconds");
  }

  hours = parsed_hours;
  minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
  seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
  milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
  this->microseconds = chrono_microseconds.count();
}

LocalTime::LocalTime(const LocalTimeParameters &local_time_parameters) {
  if (!IsInBounds(0, 23, local_time_parameters.hours)) {
    throw temporal::InvalidArgumentException("Creating a LocalTime with invalid hour parameter.");
  }

  if (!IsInBounds(0, 59, local_time_parameters.minutes)) {
    throw temporal::InvalidArgumentException("Creating a LocalTime with invalid minutes parameter.");
  }

  // ISO 8601 supports leap seconds, but we ignore it for now to simplify the implementation
  if (!IsInBounds(0, 59, local_time_parameters.seconds)) {
    throw temporal::InvalidArgumentException("Creating a LocalTime with invalid seconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.milliseconds)) {
    throw temporal::InvalidArgumentException("Creating a LocalTime with invalid milliseconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.microseconds)) {
    throw temporal::InvalidArgumentException("Creating a LocalTime with invalid microseconds parameter.");
  }

  hours = local_time_parameters.hours;
  minutes = local_time_parameters.minutes;
  seconds = local_time_parameters.seconds;
  milliseconds = local_time_parameters.milliseconds;
  microseconds = local_time_parameters.microseconds;
}

std::chrono::microseconds LocalTime::SumLocalTimeParts() const {
  namespace chrono = std::chrono;
  return chrono::hours{hours} + chrono::minutes{minutes} + chrono::seconds{seconds} +
         chrono::milliseconds{milliseconds} + chrono::microseconds{microseconds};
}

int64_t LocalTime::MicrosecondsSinceEpoch() const { return SumLocalTimeParts().count(); }

int64_t LocalTime::NanosecondsSinceEpoch() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::nanoseconds>(SumLocalTimeParts()).count();
}

size_t LocalTimeHash::operator()(const LocalTime &local_time) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, local_time.hours);
  result = hasher(result, local_time.minutes);
  result = hasher(result, local_time.seconds);
  result = hasher(result, local_time.milliseconds);
  result = hasher(result, local_time.microseconds);
  return result;
}

namespace {
constexpr auto *kSupportedLocalDateTimeFormatsHelpMessage = R"help(
String representing the LocalDateTime should be in one of the following formats:

- YYYY-MM-DDThh:mm:ss
- YYYY-MM-DDThh:mm

or

- YYYYMMDDThhmmss
- YYYYMMDDThhmm
- YYYYMMDDThh

Symbol table:
|---|---------|
| Y | YEAR    |
|---|---------|
| M | MONTH   |
|---|---------|
| D | DAY     |
|---|---------|
| h | HOURS   |
|---|---------|
| m | MINUTES |
|---|---------|
| s | SECONDS |
|---|---------|

Additionally, seconds can be defined as decimal fractions with 3 or 6 digits after the decimal point.
First 3 digits represent milliseconds, while the second 3 digits represent microseconds.

It's important to note that the date and time parts should use both the corresponding separators
or both parts should be written in their basic forms without the separators.)help";

}  // namespace
std::pair<DateParameters, LocalTimeParameters> ParseLocalDateTimeParameters(std::string_view string) {
  auto t_position = string.find('T');
  if (t_position == std::string_view::npos) {
    throw temporal::InvalidArgumentException("Invalid LocalDateTime format. {}",
                                             kSupportedLocalDateTimeFormatsHelpMessage);
  }

  try {
    auto [date_parameters, extended_date_format] = ParseDateParameters(string.substr(0, t_position));
    // https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations
    // ISO8601 specifies that you cannot mix extended and basic format of date and time
    // If the date is in the extended format, same must be true for the time, so we don't send T
    // which denotes the basic format. The opposite case also aplies.
    auto local_time_substring = string.substr(t_position + 1);
    if (local_time_substring.empty()) {
      throw temporal::InvalidArgumentException("Invalid LocalDateTime format. {}",
                                               kSupportedLocalDateTimeFormatsHelpMessage);
    }

    auto [local_time_parameters, extended_time_format] = ParseLocalTimeParameters(local_time_substring);

    if (extended_date_format ^ extended_time_format) {
      throw temporal::InvalidArgumentException(
          "Invalid LocalDateTime format. Both date and time should be in the basic or extended format. {}",
          kSupportedLocalDateTimeFormatsHelpMessage);
    }

    return {date_parameters, local_time_parameters};
  } catch (const temporal::InvalidArgumentException &e) {
    throw temporal::InvalidArgumentException("Invalid LocalDateTime format. {}",
                                             kSupportedLocalDateTimeFormatsHelpMessage);
  }
}

LocalDateTime::LocalDateTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  constexpr int64_t one_day_in_microseconds = 86400000000;
  if (chrono_microseconds.count() < 0) {
    if (chrono_microseconds.count() % one_day_in_microseconds == 0) {
      date = Date(chrono_microseconds.count());
    } else {
      date = Date(chrono_microseconds.count() - one_day_in_microseconds);
    }
  } else {
    date = Date(chrono_microseconds.count());
  }
  chrono_microseconds -= std::chrono::microseconds{date.MicrosecondsSinceEpoch()};
  local_time = LocalTime(chrono_microseconds.count());
}

int64_t LocalDateTime::MicrosecondsSinceEpoch() const {
  return date.MicrosecondsSinceEpoch() + local_time.MicrosecondsSinceEpoch();
}

int64_t LocalDateTime::SecondsSinceEpoch() const {
  namespace chrono = std::chrono;
  const auto to_sec = chrono::duration_cast<chrono::seconds>(DaysSinceEpoch(date.years, date.months, date.days));
  auto local_time_seconds =
      chrono::hours(local_time.hours) + chrono::minutes(local_time.minutes) + chrono::seconds(local_time.seconds);
  return (to_sec + local_time_seconds).count();
}

int64_t LocalDateTime::SubSecondsAsNanoseconds() const {
  namespace chrono = std::chrono;
  auto milli_as_nanos = chrono::duration_cast<chrono::nanoseconds>(chrono::milliseconds(local_time.milliseconds));
  auto micros_as_nanos = chrono::duration_cast<chrono::nanoseconds>(chrono::microseconds(local_time.microseconds));
  return (milli_as_nanos + micros_as_nanos).count();
}

LocalDateTime::LocalDateTime(const DateParameters date_parameters, const LocalTimeParameters &local_time_parameters)
    : date(date_parameters), local_time(local_time_parameters) {}

LocalDateTime::LocalDateTime(const Date &date, const LocalTime &local_time) : date(date), local_time(local_time) {}

size_t LocalDateTimeHash::operator()(const LocalDateTime &local_date_time) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, LocalTimeHash{}(local_date_time.local_time));
  result = hasher(result, DateHash{}(local_date_time.date));
  return result;
}

namespace {
std::optional<DurationParameters> TryParseDurationString(std::string_view string) {
  DurationParameters duration_parameters;

  if (string.empty()) {
    return std::nullopt;
  }

  if (string.front() != 'P') {
    return std::nullopt;
  }
  string.remove_prefix(1);

  if (string.empty()) {
    return std::nullopt;
  }

  bool decimal_point_used = false;

  const auto check_decimal_fraction = [&](const auto substring) {
    // only the last number in the string can be a fraction
    // if a decimal point was already found, and another number is being parsed
    // we are in an invalid state
    if (decimal_point_used) {
      return false;
    }

    decimal_point_used = substring.find('.') != std::string_view::npos;
    return true;
  };

  const auto parse_and_assign = [&](std::string_view &string, const char label, double &destination) {
    auto label_position = string.find(label);
    if (label_position == std::string_view::npos) {
      return true;
    }

    const auto number_substring = string.substr(0, label_position);
    if (!check_decimal_fraction(number_substring)) {
      return false;
    }

    const auto maybe_parsed_number = ParseNumber<double>(number_substring, number_substring.size());
    if (!maybe_parsed_number) {
      return false;
    }
    destination = *maybe_parsed_number;
    // remove number + label
    string.remove_prefix(label_position + 1);
    return true;
  };

  const auto parse_duration_days_part = [&](auto date_string) {
    if (!parse_and_assign(date_string, 'D', duration_parameters.days)) {
      return false;
    }

    return date_string.empty();
  };

  const auto parse_duration_time_part = [&](auto time_string) {
    if (!parse_and_assign(time_string, 'H', duration_parameters.hours)) {
      return false;
    }
    if (time_string.empty()) {
      return true;
    }

    if (!parse_and_assign(time_string, 'M', duration_parameters.minutes)) {
      return false;
    }
    if (time_string.empty()) {
      return true;
    }

    if (!parse_and_assign(time_string, 'S', duration_parameters.seconds)) {
      return false;
    }

    return time_string.empty();
  };

  auto t_position = string.find('T');

  const auto date_string = string.substr(0, t_position);
  if (!date_string.empty() && !parse_duration_days_part(date_string)) {
    return std::nullopt;
  }

  if (t_position == std::string_view::npos) {
    return duration_parameters;
  }

  const auto time_string = string.substr(t_position + 1);
  if (time_string.empty() || !parse_duration_time_part(time_string)) {
    return std::nullopt;
  }

  return duration_parameters;
}
}  // namespace

// clang-format off
const auto kSupportedDurationFormatsHelpMessage = fmt::format(R"help(
"String representing duration should be in the following format:

P[nD]T[nH][nM][nS]

Symbol table:
|---|---------|
| D | DAYS    |
|---|---------|
| H | HOURS   |
|---|---------|
| M | MINUTES |
|---|---------|
| S | SECONDS |
|---|---------|

'n' represents a number that can be an integer of ANY value, or a fraction IF it's the last value in the string.
All the fields are optional.
)help", kSupportedLocalDateTimeFormatsHelpMessage);
// clang-format on 

DurationParameters ParseDurationParameters(std::string_view string) {
  // The string needs to start with P followed by one of the two options:
  //  - string in a duration specific format
  if (string.empty() || string.front() != 'P') {
    throw temporal::InvalidArgumentException("Duration string is empty.");
  }

  if (auto maybe_duration_parameters = TryParseDurationString(string); maybe_duration_parameters) {
    return *maybe_duration_parameters;
  }

  throw utils::BasicException("Invalid duration string. {}", kSupportedDurationFormatsHelpMessage);
}

namespace {
template <Chrono From, Chrono To>
constexpr To CastChronoDouble(const double value) {
  return std::chrono::duration_cast<To>(std::chrono::duration<double, typename From::period>(value));
}; 
} // namespace

Duration::Duration(int64_t microseconds) { this->microseconds = microseconds; }

Duration::Duration(const DurationParameters &parameters) {
  microseconds = (CastChronoDouble<std::chrono::days, std::chrono::microseconds>(parameters.days) +
                  CastChronoDouble<std::chrono::hours, std::chrono::microseconds>(parameters.hours) +
                  CastChronoDouble<std::chrono::minutes, std::chrono::microseconds>(parameters.minutes) +
                  CastChronoDouble<std::chrono::seconds, std::chrono::microseconds>(parameters.seconds) +
                  CastChronoDouble<std::chrono::milliseconds, std::chrono::microseconds>(parameters.milliseconds) +
                  CastChronoDouble<std::chrono::microseconds, std::chrono::microseconds>(parameters.microseconds))
                     .count();
}

int64_t Duration::Days() const {
  std::chrono::microseconds ms(microseconds);
  return std::chrono::duration_cast<std::chrono::days>(ms).count();
}

int64_t Duration::SubDaysAsSeconds() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::seconds>(chrono::microseconds(SubDaysAsMicroseconds())).count();
}

int64_t Duration::SubDaysAsHours() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::hours>(chrono::seconds(SubDaysAsSeconds())).count();
}

int64_t Duration::SubDaysAsMinutes() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::minutes>(chrono::seconds(SubDaysAsSeconds())).count();
}

int64_t Duration::SubDaysAsMilliseconds() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::milliseconds>(chrono::microseconds(SubDaysAsMicroseconds())).count();
}

int64_t Duration::SubDaysAsNanoseconds() const {
  namespace chrono = std::chrono;
  return chrono::duration_cast<chrono::nanoseconds>(chrono::microseconds(SubDaysAsMicroseconds())).count();
}

int64_t Duration::SubDaysAsMicroseconds() const {
  namespace chrono = std::chrono;
  const auto days = chrono::days(Days()); 
  const auto micros = chrono::microseconds(microseconds);
  return (micros - days).count();
}

int64_t Duration::SubSecondsAsNanoseconds() const { 
  namespace chrono = std::chrono;
  const auto micros = chrono::microseconds(SubDaysAsMicroseconds()); 
  const auto secs = chrono::seconds(SubDaysAsSeconds());
  return chrono::duration_cast<chrono::nanoseconds>(micros - secs).count();
}

Duration Duration::operator-() const {
  if (microseconds == std::numeric_limits<decltype(microseconds)>::min()) [[unlikely]] {
      throw temporal::InvalidArgumentException("Duration arithmetic overflows");
  }
  Duration result{-microseconds};
  return result;
}

size_t DurationHash::operator()(const Duration &duration) const { return std::hash<int64_t>{}(duration.microseconds); }

}  // namespace utils
