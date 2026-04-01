#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

namespace etf {

using PriceTick = int;
using Qty = int;
using TimestampUs = std::int64_t;
using OrderId = std::uint64_t;

inline constexpr PriceTick kMinPrice = 300;
inline constexpr PriceTick kMaxPrice = 700;
inline constexpr int kMaxActiveOrdersPerSymbol = 10;

enum class Symbol { E, T, F, ETF };
enum class Side { Buy, Sell };
enum class OrderState { PendingNew, Active, PartiallyFilled, Filled, Canceled, Rejected };

struct BookLevel {
  PriceTick price{0};
  Qty qty{0};
};

struct TopOfBook {
  Symbol symbol{Symbol::E};
  std::optional<BookLevel> bid;
  std::optional<BookLevel> ask;
};

struct TradePrint {
  TimestampUs ts{0};
  Symbol symbol{Symbol::E};
  Side aggressor_side{Side::Buy};
  PriceTick price{0};
  Qty qty{0};
  std::string source;
  std::optional<OrderId> resting_order_id;
  bool team_involved{false};
};

struct Fill {
  TimestampUs ts{0};
  Symbol symbol{Symbol::E};
  OrderId order_id{0};
  Side side{Side::Buy};
  PriceTick price{0};
  Qty qty{0};
  bool aggressor{false};
  std::string source;
  std::string tag;
};

struct Ack {
  TimestampUs ts{0};
  OrderId order_id{0};
  Symbol symbol{Symbol::E};
  Side side{Side::Buy};
  PriceTick price{0};
  Qty qty{0};
  std::string tag;
};

struct CancelAck {
  TimestampUs ts{0};
  OrderId order_id{0};
  Symbol symbol{Symbol::E};
};

struct Reject {
  TimestampUs ts{0};
  std::optional<OrderId> order_id;
  Symbol symbol{Symbol::E};
  std::string reason;
};

struct Timer {
  TimestampUs ts{0};
  std::string name;
};

struct BookUpdate {
  TimestampUs ts{0};
  TopOfBook book;
};

using MarketEvent = std::variant<BookUpdate, TradePrint, Ack, Fill, CancelAck, Reject, Timer>;

struct PlaceLimit {
  OrderId order_id{0};
  Symbol symbol{Symbol::E};
  Side side{Side::Buy};
  PriceTick price{0};
  Qty qty{0};
  std::string tag;
};

struct Cancel {
  OrderId order_id{0};
  std::string reason;
};

struct CancelReplace {
  OrderId order_id{0};
  PriceTick new_price{0};
  Qty new_qty{0};
  std::string tag;
};

using OrderCommand = std::variant<PlaceLimit, Cancel, CancelReplace>;

struct OrderView {
  OrderId order_id{0};
  Symbol symbol{Symbol::E};
  Side side{Side::Buy};
  PriceTick price{0};
  Qty remaining_qty{0};
  OrderState state{OrderState::PendingNew};
  std::string tag;
};

struct Position {
  Qty qty{0};
  double avg_price{0.0};
  double realized{0.0};
  double cash{0.0};
};

struct PnL {
  double realized{0.0};
  double unrealized{0.0};
  double total{0.0};
};

struct MarketSnapshot {
  TimestampUs now{0};
  std::map<Symbol, TopOfBook> books;
  std::map<Symbol, Position> positions;
  std::unordered_map<OrderId, OrderView> active_orders;
  PnL pnl;
};

struct SimulationConfig {
  std::uint64_t seed{7};
  TimestampUs duration_us{300'000};
  int noise_bot_count{1};
  int event_bot_count{1};
  double maker_k{25.0};
  TimestampUs maker_reload_us{3'000};
  std::vector<int> maker_offsets{1, 2, 4};
  Qty maker_level_qty{8};
  TimestampUs noise_interval_us{2'000};
  TimestampUs event_interval_us{25'000};
  int event_burst_count{5};
  Qty event_base_qty{6};
  Qty noise_base_qty{3};
  TimestampUs strategy_timer_us{1'500};
  PriceTick price_min{kMinPrice};
  PriceTick price_max{kMaxPrice};
  std::map<Symbol, TopOfBook> initial_books;
  bool enable_passive_opponent{false};
  bool enable_naive_arb_opponent{false};
};

struct StrategyConfig {
  std::string name{"challenge_v1"};
  int edge_buffer_ticks{2};
  int max_abs_position{80};
  int quote_width_ticks{2};
  int hedge_unit{1};
  bool hedge_behavior{true};
  Qty event_threshold_qty{10};
  TimestampUs event_lookback_us{8'000};
  Qty aggressive_qty{6};
  double competition_share{0.20};
  bool diagnostics{true};
};

struct LiveConfig {
  bool enabled{false};
  std::string api_base_url{"https://api.comp.waterlooquantclub.com"};
  std::string websocket_url;
  std::string run_id;
  std::string access_token;
  std::vector<Symbol> subscribe_markets{Symbol::E, Symbol::T, Symbol::F, Symbol::ETF};
};

struct RunConfig {
  std::string log_output_path{"tmp/default_run.ndjson"};
  std::string replay_input_path;
  nlohmann::json sweep_overrides = nlohmann::json::array();
  bool advisory_only{false};
  LiveConfig live;
};

struct AppConfig {
  SimulationConfig simulation;
  StrategyConfig strategy;
  RunConfig run;
};

struct SummaryStats {
  std::string strategy_name;
  std::uint64_t seed{0};
  PnL pnl;
  std::map<Symbol, Qty> final_positions;
  std::map<std::string, int> fill_breakdown;
  int arb_orders{0};
  int arb_fills{0};
  int stale_quote_fills{0};
  int event_orders{0};
  double avg_event_latency_us{0.0};
};

std::array<Symbol, 4> all_symbols();
std::string to_string(Symbol symbol);
std::string to_string(Side side);
std::string to_string(OrderState state);
Symbol symbol_from_string(const std::string& value);
Side side_from_string(const std::string& value);

PriceTick validate_price_tick(PriceTick price, PriceTick min_price = kMinPrice,
                              PriceTick max_price = kMaxPrice);
void validate_active_order_limit(std::size_t active_orders_for_symbol);

double mid_price(const TopOfBook& book);
double synthetic_etf_fair(const std::map<Symbol, TopOfBook>& books);
double maker_bot_fair(double maker_position, double k);
Qty width_scaled_flow(double width, Qty base_qty);
double settlement_single(Qty noise_position, Qty event_position);
double settlement_etf(double settle_e, double settle_t, double settle_f);

void apply_fill(Position& position, Side side, PriceTick price, Qty qty);
double unrealized_for_position(const Position& position, double mark_price);

AppConfig load_config_from_path(const std::string& path);
nlohmann::json config_to_json(const AppConfig& config);

nlohmann::json symbol_to_json(Symbol symbol);
nlohmann::json side_to_json(Side side);
nlohmann::json order_state_to_json(OrderState state);
nlohmann::json top_of_book_to_json(const TopOfBook& book);
nlohmann::json event_to_json(const MarketEvent& event);
nlohmann::json command_to_json(const OrderCommand& command);
nlohmann::json snapshot_to_json(const MarketSnapshot& snapshot);
nlohmann::json summary_to_json(const SummaryStats& summary);

TimestampUs event_timestamp(const MarketEvent& event);
Symbol event_symbol(const MarketEvent& event);

}  // namespace etf
