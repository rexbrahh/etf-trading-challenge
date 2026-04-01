#include "etf/core.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <numeric>
#include <sstream>

namespace etf {

namespace {

template <typename T>
T get_or(const nlohmann::json& json, const char* key, T fallback) {
  if (json.contains(key)) {
    return json.at(key).get<T>();
  }
  return fallback;
}

TopOfBook parse_book(Symbol symbol, const nlohmann::json& json) {
  TopOfBook book;
  book.symbol = symbol;
  if (json.contains("bid") && json.contains("bid_qty")) {
    book.bid = BookLevel{json.at("bid").get<PriceTick>(), json.at("bid_qty").get<Qty>()};
  }
  if (json.contains("ask") && json.contains("ask_qty")) {
    book.ask = BookLevel{json.at("ask").get<PriceTick>(), json.at("ask_qty").get<Qty>()};
  }
  return book;
}

double roundtrip_mid(const std::optional<BookLevel>& first, const std::optional<BookLevel>& second) {
  if (first && second) {
    return (first->price + second->price) / 2.0;
  }
  if (first) {
    return static_cast<double>(first->price);
  }
  if (second) {
    return static_cast<double>(second->price);
  }
  return 500.0;
}

}  // namespace

std::array<Symbol, 4> all_symbols() { return {Symbol::E, Symbol::T, Symbol::F, Symbol::ETF}; }

std::string to_string(Symbol symbol) {
  switch (symbol) {
    case Symbol::E:
      return "E";
    case Symbol::T:
      return "T";
    case Symbol::F:
      return "F";
    case Symbol::ETF:
      return "ETF";
  }
  throw std::invalid_argument("unknown symbol");
}

std::string to_string(Side side) {
  switch (side) {
    case Side::Buy:
      return "buy";
    case Side::Sell:
      return "sell";
  }
  throw std::invalid_argument("unknown side");
}

std::string to_string(OrderState state) {
  switch (state) {
    case OrderState::PendingNew:
      return "pending_new";
    case OrderState::Active:
      return "active";
    case OrderState::PartiallyFilled:
      return "partially_filled";
    case OrderState::Filled:
      return "filled";
    case OrderState::Canceled:
      return "canceled";
    case OrderState::Rejected:
      return "rejected";
  }
  throw std::invalid_argument("unknown order state");
}

Symbol symbol_from_string(const std::string& value) {
  if (value == "E") {
    return Symbol::E;
  }
  if (value == "T") {
    return Symbol::T;
  }
  if (value == "F") {
    return Symbol::F;
  }
  if (value == "ETF") {
    return Symbol::ETF;
  }
  throw std::invalid_argument("invalid symbol: " + value);
}

Side side_from_string(const std::string& value) {
  if (value == "buy" || value == "Buy") {
    return Side::Buy;
  }
  if (value == "sell" || value == "Sell") {
    return Side::Sell;
  }
  throw std::invalid_argument("invalid side: " + value);
}

PriceTick validate_price_tick(PriceTick price, PriceTick min_price, PriceTick max_price) {
  if (price < min_price || price > max_price) {
    throw std::out_of_range("price outside allowed band");
  }
  return price;
}

void validate_active_order_limit(std::size_t active_orders_for_symbol) {
  if (active_orders_for_symbol >= static_cast<std::size_t>(kMaxActiveOrdersPerSymbol)) {
    throw std::runtime_error("active order limit exceeded");
  }
}

double mid_price(const TopOfBook& book) { return roundtrip_mid(book.bid, book.ask); }

double synthetic_etf_fair(const std::map<Symbol, TopOfBook>& books) {
  const auto price_e = mid_price(books.at(Symbol::E));
  const auto price_t = mid_price(books.at(Symbol::T));
  const auto price_f = mid_price(books.at(Symbol::F));
  return (price_e + 2.0 * price_t + 3.0 * price_f) / 6.0;
}

double maker_bot_fair(double maker_position, double k) { return 500.0 - maker_position / k; }

Qty width_scaled_flow(double width, Qty base_qty) {
  const auto clipped_width = std::max(1.0, width);
  const auto multiplier = std::clamp(static_cast<int>(std::round(8.0 - clipped_width)), 1, 6);
  return std::max(1, base_qty * multiplier);
}

double settlement_single(Qty noise_position, Qty event_position) {
  return 500.0 + static_cast<double>(noise_position + event_position) / 100.0;
}

double settlement_etf(double settle_e, double settle_t, double settle_f) {
  return (settle_e + 2.0 * settle_t + 3.0 * settle_f) / 6.0;
}

void apply_fill(Position& position, Side side, PriceTick price, Qty qty) {
  const auto signed_qty = side == Side::Buy ? qty : -qty;

  if (position.qty == 0 || (position.qty > 0 && signed_qty > 0) || (position.qty < 0 && signed_qty < 0)) {
    const auto current_abs = std::abs(position.qty);
    const auto fill_abs = std::abs(signed_qty);
    position.avg_price =
        ((position.avg_price * current_abs) + (static_cast<double>(price) * fill_abs)) /
        static_cast<double>(current_abs + fill_abs);
    position.qty += signed_qty;
  } else {
    const auto closing_qty = std::min(std::abs(position.qty), std::abs(signed_qty));
    if (position.qty > 0) {
      position.realized += (static_cast<double>(price) - position.avg_price) * closing_qty;
    } else {
      position.realized += (position.avg_price - static_cast<double>(price)) * closing_qty;
    }
    position.qty += signed_qty;
    if (position.qty == 0) {
      position.avg_price = 0.0;
    } else if ((position.qty > 0 && signed_qty > 0) || (position.qty < 0 && signed_qty < 0)) {
      position.avg_price = static_cast<double>(price);
    }
  }

  if (side == Side::Buy) {
    position.cash -= static_cast<double>(price * qty);
  } else {
    position.cash += static_cast<double>(price * qty);
  }
}

double unrealized_for_position(const Position& position, double mark_price) {
  if (position.qty > 0) {
    return (mark_price - position.avg_price) * position.qty;
  }
  if (position.qty < 0) {
    return (position.avg_price - mark_price) * std::abs(position.qty);
  }
  return 0.0;
}

AppConfig load_config_from_path(const std::string& path) {
  std::ifstream in(path);
  if (!in.is_open()) {
    throw std::runtime_error("unable to open config: " + path);
  }
  nlohmann::json json;
  in >> json;

  AppConfig config;
  const auto& sim = json.at("simulation");
  const auto& strategy = json.at("strategy");
  const auto& run = json.at("run");

  config.simulation.seed = get_or(sim, "seed", config.simulation.seed);
  config.simulation.duration_us = get_or(sim, "duration_us", config.simulation.duration_us);
  config.simulation.noise_bot_count = get_or(sim, "noise_bot_count", config.simulation.noise_bot_count);
  config.simulation.event_bot_count = get_or(sim, "event_bot_count", config.simulation.event_bot_count);
  config.simulation.maker_k = get_or(sim, "maker_k", config.simulation.maker_k);
  config.simulation.maker_reload_us = get_or(sim, "maker_reload_us", config.simulation.maker_reload_us);
  config.simulation.maker_offsets =
      get_or(sim, "maker_offsets", config.simulation.maker_offsets);
  config.simulation.maker_level_qty =
      get_or(sim, "maker_level_qty", config.simulation.maker_level_qty);
  config.simulation.noise_interval_us =
      get_or(sim, "noise_interval_us", config.simulation.noise_interval_us);
  config.simulation.event_interval_us =
      get_or(sim, "event_interval_us", config.simulation.event_interval_us);
  config.simulation.event_burst_count =
      get_or(sim, "event_burst_count", config.simulation.event_burst_count);
  config.simulation.event_base_qty = get_or(sim, "event_base_qty", config.simulation.event_base_qty);
  config.simulation.noise_base_qty = get_or(sim, "noise_base_qty", config.simulation.noise_base_qty);
  config.simulation.strategy_timer_us =
      get_or(sim, "strategy_timer_us", config.simulation.strategy_timer_us);
  config.simulation.price_min = get_or(sim, "price_min", config.simulation.price_min);
  config.simulation.price_max = get_or(sim, "price_max", config.simulation.price_max);
  config.simulation.enable_passive_opponent =
      get_or(sim, "enable_passive_opponent", config.simulation.enable_passive_opponent);
  config.simulation.enable_naive_arb_opponent =
      get_or(sim, "enable_naive_arb_opponent", config.simulation.enable_naive_arb_opponent);

  if (sim.contains("initial_book_state")) {
    for (const auto& symbol : all_symbols()) {
      const auto key = to_string(symbol);
      if (sim.at("initial_book_state").contains(key)) {
        config.simulation.initial_books[symbol] = parse_book(symbol, sim.at("initial_book_state").at(key));
      }
    }
  }

  strategy.at("name").get_to(config.strategy.name);
  config.strategy.edge_buffer_ticks =
      get_or(strategy, "edge_buffer_ticks", config.strategy.edge_buffer_ticks);
  config.strategy.max_abs_position =
      get_or(strategy, "max_abs_position", config.strategy.max_abs_position);
  config.strategy.quote_width_ticks =
      get_or(strategy, "quote_width_ticks", config.strategy.quote_width_ticks);
  config.strategy.hedge_unit = get_or(strategy, "hedge_unit", config.strategy.hedge_unit);
  config.strategy.hedge_behavior = get_or(strategy, "hedge_behavior", config.strategy.hedge_behavior);
  config.strategy.event_threshold_qty =
      get_or(strategy, "event_threshold_qty", config.strategy.event_threshold_qty);
  config.strategy.event_lookback_us =
      get_or(strategy, "event_lookback_us", config.strategy.event_lookback_us);
  config.strategy.aggressive_qty = get_or(strategy, "aggressive_qty", config.strategy.aggressive_qty);
  config.strategy.competition_share =
      get_or(strategy, "competition_share", config.strategy.competition_share);
  config.strategy.diagnostics = get_or(strategy, "diagnostics", config.strategy.diagnostics);

  config.run.log_output_path = get_or(run, "log_output_path", config.run.log_output_path);
  config.run.replay_input_path = get_or(run, "replay_input_path", config.run.replay_input_path);
  config.run.sweep_overrides = get_or(run, "sweep_overrides", config.run.sweep_overrides);
  if (run.contains("live")) {
    const auto& live = run.at("live");
    config.run.live.enabled = get_or(live, "enabled", config.run.live.enabled);
    config.run.live.api_base_url = get_or(live, "api_base_url", config.run.live.api_base_url);
    config.run.live.websocket_url = get_or(live, "websocket_url", config.run.live.websocket_url);
    config.run.live.run_id = get_or(live, "run_id", config.run.live.run_id);
    config.run.live.access_token = get_or(live, "access_token", config.run.live.access_token);
    if (live.contains("subscribe_markets")) {
      config.run.live.subscribe_markets.clear();
      for (const auto& symbol : live.at("subscribe_markets")) {
        config.run.live.subscribe_markets.push_back(symbol_from_string(symbol.get<std::string>()));
      }
    }
  }

  if (config.simulation.initial_books.empty()) {
    for (const auto& symbol : all_symbols()) {
      config.simulation.initial_books[symbol] = TopOfBook{
          symbol,
          BookLevel{499, 10},
          BookLevel{501, 10},
      };
    }
  }

  return config;
}

nlohmann::json config_to_json(const AppConfig& config) {
  nlohmann::json initial_books = nlohmann::json::object();
  for (const auto& [symbol, book] : config.simulation.initial_books) {
    initial_books[to_string(symbol)] = {
        {"bid", book.bid ? book.bid->price : 0},
        {"bid_qty", book.bid ? book.bid->qty : 0},
        {"ask", book.ask ? book.ask->price : 0},
        {"ask_qty", book.ask ? book.ask->qty : 0},
    };
  }
  nlohmann::json subscribe_markets = nlohmann::json::array();
  for (const auto symbol : config.run.live.subscribe_markets) {
    subscribe_markets.push_back(to_string(symbol));
  }

  return {
      {"simulation",
       {
           {"seed", config.simulation.seed},
           {"duration_us", config.simulation.duration_us},
           {"noise_bot_count", config.simulation.noise_bot_count},
           {"event_bot_count", config.simulation.event_bot_count},
           {"maker_k", config.simulation.maker_k},
           {"maker_reload_us", config.simulation.maker_reload_us},
           {"maker_offsets", config.simulation.maker_offsets},
           {"maker_level_qty", config.simulation.maker_level_qty},
           {"noise_interval_us", config.simulation.noise_interval_us},
           {"event_interval_us", config.simulation.event_interval_us},
           {"event_burst_count", config.simulation.event_burst_count},
           {"event_base_qty", config.simulation.event_base_qty},
           {"noise_base_qty", config.simulation.noise_base_qty},
           {"strategy_timer_us", config.simulation.strategy_timer_us},
           {"price_min", config.simulation.price_min},
           {"price_max", config.simulation.price_max},
           {"enable_passive_opponent", config.simulation.enable_passive_opponent},
           {"enable_naive_arb_opponent", config.simulation.enable_naive_arb_opponent},
           {"initial_book_state", initial_books},
       }},
      {"strategy",
       {
           {"name", config.strategy.name},
           {"edge_buffer_ticks", config.strategy.edge_buffer_ticks},
           {"max_abs_position", config.strategy.max_abs_position},
           {"quote_width_ticks", config.strategy.quote_width_ticks},
           {"hedge_unit", config.strategy.hedge_unit},
           {"hedge_behavior", config.strategy.hedge_behavior},
           {"event_threshold_qty", config.strategy.event_threshold_qty},
           {"event_lookback_us", config.strategy.event_lookback_us},
           {"aggressive_qty", config.strategy.aggressive_qty},
           {"competition_share", config.strategy.competition_share},
           {"diagnostics", config.strategy.diagnostics},
       }},
      {"run",
       {
           {"log_output_path", config.run.log_output_path},
           {"replay_input_path", config.run.replay_input_path},
           {"sweep_overrides", config.run.sweep_overrides},
           {"live",
            {
                {"enabled", config.run.live.enabled},
                {"api_base_url", config.run.live.api_base_url},
                {"websocket_url", config.run.live.websocket_url},
                {"run_id", config.run.live.run_id},
                {"access_token", config.run.live.access_token},
                {"subscribe_markets", subscribe_markets},
            }},
       }},
  };
}

nlohmann::json symbol_to_json(Symbol symbol) { return to_string(symbol); }

nlohmann::json side_to_json(Side side) { return to_string(side); }

nlohmann::json order_state_to_json(OrderState state) { return to_string(state); }

nlohmann::json top_of_book_to_json(const TopOfBook& book) {
  return {
      {"symbol", to_string(book.symbol)},
      {"bid", book.bid ? nlohmann::json{{"price", book.bid->price}, {"qty", book.bid->qty}}
                        : nlohmann::json()},
      {"ask", book.ask ? nlohmann::json{{"price", book.ask->price}, {"qty", book.ask->qty}}
                        : nlohmann::json()},
  };
}

nlohmann::json event_to_json(const MarketEvent& event) {
  return std::visit(
      [](const auto& payload) -> nlohmann::json {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, BookUpdate>) {
          return {
              {"type", "book_update"},
              {"ts", payload.ts},
              {"book", top_of_book_to_json(payload.book)},
          };
        } else if constexpr (std::is_same_v<T, TradePrint>) {
          return {
              {"type", "trade_print"},
              {"ts", payload.ts},
              {"symbol", to_string(payload.symbol)},
              {"aggressor_side", to_string(payload.aggressor_side)},
              {"price", payload.price},
              {"qty", payload.qty},
              {"source", payload.source},
              {"resting_order_id", payload.resting_order_id ? nlohmann::json(*payload.resting_order_id)
                                                            : nlohmann::json()},
              {"team_involved", payload.team_involved},
          };
        } else if constexpr (std::is_same_v<T, Ack>) {
          return {
              {"type", "ack"},
              {"ts", payload.ts},
              {"order_id", payload.order_id},
              {"symbol", to_string(payload.symbol)},
              {"side", to_string(payload.side)},
              {"price", payload.price},
              {"qty", payload.qty},
              {"tag", payload.tag},
          };
        } else if constexpr (std::is_same_v<T, Fill>) {
          return {
              {"type", "fill"},
              {"ts", payload.ts},
              {"order_id", payload.order_id},
              {"symbol", to_string(payload.symbol)},
              {"side", to_string(payload.side)},
              {"price", payload.price},
              {"qty", payload.qty},
              {"aggressor", payload.aggressor},
              {"source", payload.source},
              {"tag", payload.tag},
          };
        } else if constexpr (std::is_same_v<T, CancelAck>) {
          return {
              {"type", "cancel_ack"},
              {"ts", payload.ts},
              {"order_id", payload.order_id},
              {"symbol", to_string(payload.symbol)},
          };
        } else if constexpr (std::is_same_v<T, Reject>) {
          return {
              {"type", "reject"},
              {"ts", payload.ts},
              {"order_id", payload.order_id ? nlohmann::json(*payload.order_id) : nlohmann::json()},
              {"symbol", to_string(payload.symbol)},
              {"reason", payload.reason},
          };
        } else {
          return {
              {"type", "timer"},
              {"ts", payload.ts},
              {"name", payload.name},
          };
        }
      },
      event);
}

nlohmann::json command_to_json(const OrderCommand& command) {
  return std::visit(
      [](const auto& payload) -> nlohmann::json {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, PlaceLimit>) {
          return {
              {"type", "place_limit"},
              {"order_id", payload.order_id},
              {"symbol", to_string(payload.symbol)},
              {"side", to_string(payload.side)},
              {"price", payload.price},
              {"qty", payload.qty},
              {"tag", payload.tag},
          };
        } else if constexpr (std::is_same_v<T, Cancel>) {
          return {
              {"type", "cancel"},
              {"order_id", payload.order_id},
              {"reason", payload.reason},
          };
        } else {
          return {
              {"type", "cancel_replace"},
              {"order_id", payload.order_id},
              {"new_price", payload.new_price},
              {"new_qty", payload.new_qty},
              {"tag", payload.tag},
          };
        }
      },
      command);
}

nlohmann::json snapshot_to_json(const MarketSnapshot& snapshot) {
  nlohmann::json books = nlohmann::json::object();
  for (const auto& [symbol, book] : snapshot.books) {
    books[to_string(symbol)] = top_of_book_to_json(book);
  }

  nlohmann::json positions = nlohmann::json::object();
  for (const auto& [symbol, position] : snapshot.positions) {
    positions[to_string(symbol)] = {
        {"qty", position.qty},
        {"avg_price", position.avg_price},
        {"realized", position.realized},
        {"cash", position.cash},
    };
  }

  nlohmann::json orders = nlohmann::json::array();
  for (const auto& [id, order] : snapshot.active_orders) {
    orders.push_back({
        {"order_id", id},
        {"symbol", to_string(order.symbol)},
        {"side", to_string(order.side)},
        {"price", order.price},
        {"remaining_qty", order.remaining_qty},
        {"state", to_string(order.state)},
        {"tag", order.tag},
    });
  }

  return {
      {"ts", snapshot.now},
      {"books", books},
      {"positions", positions},
      {"active_orders", orders},
      {"pnl",
       {{"realized", snapshot.pnl.realized},
        {"unrealized", snapshot.pnl.unrealized},
        {"total", snapshot.pnl.total}}},
  };
}

nlohmann::json summary_to_json(const SummaryStats& summary) {
  nlohmann::json positions = nlohmann::json::object();
  for (const auto& [symbol, qty] : summary.final_positions) {
    positions[to_string(symbol)] = qty;
  }
  return {
      {"strategy_name", summary.strategy_name},
      {"seed", summary.seed},
      {"pnl",
       {{"realized", summary.pnl.realized},
        {"unrealized", summary.pnl.unrealized},
        {"total", summary.pnl.total}}},
      {"final_positions", positions},
      {"fill_breakdown", summary.fill_breakdown},
      {"arb_orders", summary.arb_orders},
      {"arb_fills", summary.arb_fills},
      {"stale_quote_fills", summary.stale_quote_fills},
      {"event_orders", summary.event_orders},
      {"avg_event_latency_us", summary.avg_event_latency_us},
  };
}

TimestampUs event_timestamp(const MarketEvent& event) {
  return std::visit([](const auto& payload) { return payload.ts; }, event);
}

Symbol event_symbol(const MarketEvent& event) {
  return std::visit(
      [](const auto& payload) -> Symbol {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, BookUpdate>) {
          return payload.book.symbol;
        } else if constexpr (std::is_same_v<T, TradePrint>) {
          return payload.symbol;
        } else if constexpr (std::is_same_v<T, Ack>) {
          return payload.symbol;
        } else if constexpr (std::is_same_v<T, Fill>) {
          return payload.symbol;
        } else if constexpr (std::is_same_v<T, CancelAck>) {
          return payload.symbol;
        } else if constexpr (std::is_same_v<T, Reject>) {
          return payload.symbol;
        } else {
          return Symbol::ETF;
        }
      },
      event);
}

}  // namespace etf
