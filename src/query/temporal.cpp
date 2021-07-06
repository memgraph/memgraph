#include "query/temporal.hpp"

#include <charconv>
#include <chrono>
#include <string_view>

#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace query {
namespace {
template <typename T>
concept Chrono = requires(T) {
  typename T::rep;
  typename T::period;
};

template <Chrono TFirst, Chrono TSecond>
constexpr auto GetAndSubtractDuration(TSecond &base_duration) {
  const auto duration = std::chrono::duration_cast<TFirst>(base_duration);
  base_duration -= duration;
  return duration.count();
}

constexpr std::chrono::microseconds epoch{std::chrono::years{1970} + std::chrono::months{1} + std::chrono::days{1}};

constexpr bool IsInBounds(const auto low, const auto high, const auto value) { return low <= value && value <= high; }

constexpr bool IsValidDay(const auto day, const auto month, const auto year) {
  constexpr std::array<uint8_t, 12> days{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  MG_ASSERT(IsInBounds(1, 12, month), "Invalid month!");

  if (day <= 0) {
    return false;
  }

  const auto is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

  uint8_t leap_day = month == 2 && is_leap_year;

  return day <= (days[month - 1] + leap_day);
}

template <typename T>
std::optional<T> ParseNumber(const std::string_view string, const size_t size) {
  if (string.size() < size) {
    return std::nullopt;
  }

  T value{};
  if (const auto [p, ec] = std::from_chars(string.data(), string.data() + size, value);
      static_cast<bool>(ec) || p != string.data() + size) {
    return std::nullopt;
  }

  return value;
}

}  // namespace

Date::Date(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  chrono_microseconds += epoch;
  MG_ASSERT(chrono_microseconds.count() >= 0, "Invalid Date specified in microseconds");
  years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
  months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
  days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
}

Date::Date(const DateParameters &date_parameters) {
  if (!IsInBounds(0, 9999, date_parameters.years)) {
    throw utils::BasicException("Creating a Date with invalid year parameter.");
  }

  // TODO(antonio2368): Replace with year_month_day when it's implemented
  // https://en.cppreference.com/w/cpp/chrono/year_month_day/ok
  if (!IsInBounds(1, 12, date_parameters.months)) {
    throw utils::BasicException("Creating a Date with invalid month parameter.");
  }

  if (!IsValidDay(date_parameters.days, date_parameters.months, date_parameters.years)) {
    throw utils::BasicException("Creating a Date with invalid day parameter.");
  }

  years = date_parameters.years;
  months = date_parameters.months;
  days = date_parameters.days;
}

std::pair<DateParameters, bool> ParseDateParameters(std::string_view date_string) {
  constexpr std::array valid_sizes{
      10,  // YYYY-MM-DD
      8,   // YYYYMMDD
      7    // YYYY-MM
  };
  if (!std::any_of(
          valid_sizes.begin(), valid_sizes.end(),
          [date_string_size = date_string.size()](const auto valid_size) { return valid_size == date_string_size; })) {
    throw utils::BasicException("Invalid string for date");
  }

  DateParameters date_parameters;
  auto maybe_year = ParseNumber<int64_t>(date_string, 4);
  if (!maybe_year) {
    throw utils::BasicException("Invalid year in the string");
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
    throw utils::BasicException("Invalid month in the string");
  }
  date_parameters.months = *maybe_month;
  date_string.remove_prefix(2);

  if (!date_string.empty()) {
    if (date_string.front() == '-') {
      if (!is_extended_format) {
        throw utils::BasicException("Invalid format for the date");
      }
      date_string.remove_prefix(1);
    }

    auto maybe_day = ParseNumber<int64_t>(date_string, 2);
    if (!maybe_day) {
      throw utils::BasicException("Invalid month in the string");
    }
    date_parameters.days = *maybe_day;
    date_string.remove_prefix(2);
  }

  if (!date_string.empty()) {
    throw utils::BasicException("Invalid format for the date");
  }

  return std::make_pair(date_parameters, is_extended_format);
}

int64_t Date::MicrosecondsSinceEpoch() const {
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days});

  result -= epoch;
  return result.count();
}

size_t DateHash::operator()(const Date &date) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, date.years);
  result = hasher(result, date.months);
  result = hasher(result, date.days);
  return result;
}

LocalTimeParameters ParseLocalTimeParameters(std::string_view local_time_string) {
  // supported formats:
  //  hh:mm:ss.ssssss | Thhmmss.ssssss
  //  hh:mm:ss.sss    | Thhmmss.sss
  //  hh:mm           | Thhmm
  //  Thh
  const bool has_t = local_time_string.front() == 'T';
  if (has_t) {
    local_time_string.remove_prefix(1);
  }

  const auto process_colon = [has_t, &local_time_string] {
    // We cannot have 'T' and ':' as a separator at the same time
    if (!(has_t ^ (local_time_string.front() == ':'))) {
      throw utils::BasicException("Invalid format for the local time");
    }

    if (has_t) {
      return;
    }

    local_time_string.remove_prefix(1);
    if (local_time_string.empty()) {
      throw utils::BasicException("Invalid format for the local time");
    }
  };

  LocalTimeParameters local_time_parameters;

  const auto maybe_hour = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_hour) {
    throw utils::BasicException("Invalid hour in the string");
  }
  local_time_parameters.hours = *maybe_hour;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return local_time_parameters;
  }

  process_colon();

  const auto maybe_minute = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_minute) {
    throw utils::BasicException("Invalid minutes in the string");
  }
  local_time_parameters.minutes = *maybe_minute;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return local_time_parameters;
  }

  process_colon();

  const auto maybe_seconds = ParseNumber<int64_t>(local_time_string, 2);
  if (!maybe_seconds) {
    throw utils::BasicException("Invalid seconds in the string");
  }
  local_time_parameters.seconds = *maybe_seconds;
  local_time_string.remove_prefix(2);

  if (local_time_string.empty()) {
    return local_time_parameters;
  }

  if (local_time_string.front() != '.') {
    throw utils::BasicException("Invalid format for local time");
  }
  local_time_string.remove_prefix(1);

  const auto maybe_milliseconds = ParseNumber<int64_t>(local_time_string, 3);
  if (!maybe_milliseconds) {
    throw utils::BasicException("Invalid milliseconds in the string");
  }
  local_time_parameters.milliseconds = *maybe_milliseconds;
  local_time_string.remove_prefix(3);

  if (local_time_string.empty()) {
    return local_time_parameters;
  }

  const auto maybe_microseconds = ParseNumber<int64_t>(local_time_string, 3);
  if (!maybe_microseconds) {
    throw utils::BasicException("Invalid microseconds in the string");
  }
  local_time_parameters.microseconds = *maybe_microseconds;
  local_time_string.remove_prefix(3);

  return local_time_parameters;
}

LocalTime::LocalTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  MG_ASSERT(chrono_microseconds.count() >= 0, "Negative LocalTime specified in microseconds");

  const auto parsed_hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
  MG_ASSERT(parsed_hours <= 23, "Invalid LocalTime specified in microseconds");

  hours = parsed_hours;
  minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
  seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
  milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
  this->microseconds = chrono_microseconds.count();
}

LocalTime::LocalTime(const LocalTimeParameters &local_time_parameters) {
  if (!IsInBounds(0, 23, local_time_parameters.hours)) {
    throw utils::BasicException("Creating a LocalTime with invalid hour parameter.");
  }

  if (!IsInBounds(0, 59, local_time_parameters.minutes)) {
    throw utils::BasicException("Creating a LocalTime with invalid minutes parameter.");
  }

  if (!IsInBounds(0, 59, local_time_parameters.seconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.milliseconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.microseconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  hours = local_time_parameters.hours;
  minutes = local_time_parameters.minutes;
  seconds = local_time_parameters.seconds;
  milliseconds = local_time_parameters.milliseconds;
  microseconds = local_time_parameters.microseconds;
}

int64_t LocalTime::MicrosecondsSinceEpoch() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::hours{hours} + std::chrono::minutes{minutes} + std::chrono::seconds{seconds} +
             std::chrono::milliseconds{milliseconds} + std::chrono::microseconds{microseconds})
      .count();
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

std::pair<DateParameters, LocalTimeParameters> ParseLocalDateTimeParameters(std::string_view string) {
  auto t_position = string.find('T');
  if (t_position == std::string_view::npos) {
    throw utils::BasicException("Invalid local date time format");
  }

  auto [date_parameters, is_extended_format] = ParseDateParameters(string.substr(0, t_position));
  // ISO8601 specifies that you cannot mix extended and basic format of date and time
  // If the date is in the extended format, same must be true for the time, so we don't send T
  // which denotes the basic format. The opposite case also aplies.
  auto local_time_substring = string.substr(is_extended_format ? t_position + 1 : t_position);
  if (local_time_substring.empty()) {
    throw utils::BasicException("Invalid local date time format");
  }

  auto local_time_parameters = ParseLocalTimeParameters(local_time_substring);

  return {date_parameters, local_time_parameters};
}

LocalDateTime::LocalDateTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  date = Date(chrono_microseconds.count());
  chrono_microseconds -= std::chrono::microseconds{date.MicrosecondsSinceEpoch()};
  local_time = LocalTime(chrono_microseconds.count());
}

// return microseconds normilized with regard to epoch time point
int64_t LocalDateTime::MicrosecondsSinceEpoch() const {
  return date.MicrosecondsSinceEpoch() + local_time.MicrosecondsSinceEpoch();
}

LocalDateTime::LocalDateTime(const DateParameters date_parameters, const LocalTimeParameters &local_time_parameters)
    : date(date_parameters), local_time(local_time_parameters) {}

size_t LocalDateTimeHash::operator()(const LocalDateTime &local_date_time) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, LocalTimeHash{}(local_date_time.local_time));
  result = hasher(result, DateHash{}(local_date_time.date));
  return result;
}

namespace {
std::optional<DurationParameters> TryParseIsoDurationString(std::string_view string) {
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
    if (decimal_point_used) {
      return false;
    }

    decimal_point_used = substring.find('.') != std::string_view::npos;
    return true;
  };

  const auto parse_and_assign = [&](auto &string, const char label, double &destination) {
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

  const auto parse_duration_date_part = [&](auto date_string) {
    if (!std::isdigit(date_string.front())) {
      return false;
      throw utils::BasicException("Invalid format of duration string");
    }

    if (!parse_and_assign(date_string, 'Y', duration_parameters.years)) {
      return false;
    }
    if (date_string.empty()) {
      return true;
    }

    if (!parse_and_assign(date_string, 'M', duration_parameters.months)) {
      return false;
    }
    if (date_string.empty()) {
      return true;
    }

    if (!parse_and_assign(date_string, 'D', duration_parameters.days)) {
      return false;
    }

    return date_string.empty();
  };

  const auto parse_duration_time_part = [&](auto time_string) {
    if (!std::isdigit(time_string.front())) {
      return false;
    }

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
  if (!date_string.empty() && !parse_duration_date_part(date_string)) {
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

DurationParameters ParseDurationParameters(std::string_view string) {
  if (string.empty() || string.front() != 'P') {
    throw utils::BasicException("Duration string is empty");
  }

  if (auto maybe_duration_parameters = TryParseIsoDurationString(string); maybe_duration_parameters) {
    return *maybe_duration_parameters;
  }

  DurationParameters duration_parameters;
  // remove P and try to parse local date time
  string.remove_prefix(1);

  const auto [date_parameters, local_time_parameters] = ParseLocalDateTimeParameters(string);

  duration_parameters.years = date_parameters.years;
  duration_parameters.months = date_parameters.months;
  duration_parameters.days = date_parameters.days;
  duration_parameters.hours = local_time_parameters.hours;
  duration_parameters.minutes = local_time_parameters.minutes;
  duration_parameters.seconds = local_time_parameters.seconds;

  return duration_parameters;
}

Duration::Duration(int64_t microseconds) { this->microseconds = microseconds; }

namespace {
template <Chrono From, Chrono To>
constexpr To CastChronoDouble(const double value) {
  return std::chrono::duration_cast<To>(std::chrono::duration<double, typename From::period>(value));
};

}  // namespace

Duration::Duration(const DurationParameters &parameters) {
  microseconds = (CastChronoDouble<std::chrono::years, std::chrono::microseconds>(parameters.years) +
                  CastChronoDouble<std::chrono::months, std::chrono::microseconds>(parameters.months) +
                  CastChronoDouble<std::chrono::days, std::chrono::microseconds>(parameters.days) +
                  CastChronoDouble<std::chrono::hours, std::chrono::microseconds>(parameters.hours) +
                  CastChronoDouble<std::chrono::minutes, std::chrono::microseconds>(parameters.minutes) +
                  CastChronoDouble<std::chrono::seconds, std::chrono::microseconds>(parameters.seconds))
                     .count();
}

Duration Duration::operator-() const {
  Duration result{-microseconds};
  return result;
}

size_t DurationHash::operator()(const Duration &duration) const { return std::hash<int64_t>{}(duration.microseconds); }

}  // namespace query
