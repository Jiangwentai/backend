#include "market_data/market_tick.hpp"
#include <algorithm>
#include <charconv>
#include <cstring>
#include <stdexcept>

namespace market_data {
template <std::size_t N> void FixedString<N>::assign(std::string_view value) noexcept {
  data.fill('\0'); const auto n = std::min(value.size(), N - 1); std::memcpy(data.data(), value.data(), n);
}
template <std::size_t N> std::string_view FixedString<N>::view() const noexcept {
  return {data.data(), std::char_traits<char>::length(data.data())};
}
template struct FixedString<37>; template struct FixedString<9>; template struct FixedString<32>;

bool is_valid_price(double v) noexcept { return std::isfinite(v) && std::abs(v) < 1.0e100; }
double normalize_price(double v) noexcept { return is_valid_price(v) ? v : std::numeric_limits<double>::quiet_NaN(); }

namespace {
int parse_int(std::string_view s) { int v{}; auto [p, ec] = std::from_chars(s.data(), s.data()+s.size(), v); if (ec != std::errc{} || p != s.data()+s.size()) throw std::invalid_argument("invalid CTP timestamp field"); return v; }
std::chrono::sys_days parse_day(std::string_view d) {
  if (d.size()!=8) throw std::invalid_argument("action_day must be YYYYMMDD");
  using namespace std::chrono; year_month_day ymd{year{parse_int(d.substr(0,4))}, month{static_cast<unsigned>(parse_int(d.substr(4,2)))}, day{static_cast<unsigned>(parse_int(d.substr(6,2)))}};
  if (!ymd.ok()) throw std::invalid_argument("invalid action_day");
  return sys_days{ymd};
}
}
std::int64_t normalize_ctp_event_ts_us(std::string_view action_day, std::string_view t, int ms, std::chrono::system_clock::time_point recv) {
  using namespace std::chrono;
  if (t.size()!=8 || t[2]!=':' || t[5]!=':' || ms<0 || ms>999) throw std::invalid_argument("invalid update time");
  const auto hour=parse_int(t.substr(0,2)),minute=parse_int(t.substr(3,2)),second=parse_int(t.substr(6,2));
  if(hour<0||hour>23||minute<0||minute>59||second<0||second>59)throw std::invalid_argument("invalid update time");
  const auto tod = hours{hour}+minutes{minute}+seconds{second}+milliseconds{ms};
  constexpr auto china_offset = hours{8};
  const auto local_recv = recv + china_offset;
  sys_days base = action_day.empty() ? floor<days>(local_recv) : parse_day(action_day);
  auto local_event = base + tod;
  if (action_day.empty()) {
    const auto delta = local_event - local_recv;
    if (delta > hours{12}) local_event -= days{1};
    else if (delta < -hours{12}) local_event += days{1};
  }
  return duration_cast<microseconds>((local_event - china_offset).time_since_epoch()).count();
}
}
