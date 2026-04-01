#include "etf/simulator.hpp"

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>

#include "etf/logging.hpp"

namespace etf {

namespace {

std::string owner_for_maker(Symbol symbol) { return "maker:" + to_string(symbol); }
std::string owner_for_passive_opponent(Symbol symbol) { return "opp_passive:" + to_string(symbol); }

bool is_team_tag(const std::string& tag, const std::string& needle) { return tag.find(needle) != std::string::npos; }

}  // namespace

Simulator::Simulator(AppConfig config, RunLogger* logger)
    : config_(std::move(config)), logger_(logger), rng_(config_.simulation.seed) {
  summary_.seed = config_.simulation.seed;
  summary_.strategy_name = config_.strategy.name;
}

void Simulator::connect() {
  if (connected_) {
    return;
  }
  connected_ = true;
  finished_ = false;
  now_ = 0;
  snapshot_.now = 0;
  for (const auto symbol : all_symbols()) {
    snapshot_.positions[symbol] = {};
    books_[symbol].current = config_.simulation.initial_books.at(symbol);
    books_[symbol].last_emitted = {};
    maker_position_[symbol] = 0.0;
    maker_ready_ts_[symbol] = 0;
    noise_positions_[symbol] = 0;
    event_positions_[symbol] = 0;
  }
  if (logger_) {
    logger_->log_config(config_);
  }
  initialize_books();
  schedule_initial_actions();
}

void Simulator::disconnect() { finished_ = true; }

void Simulator::flush() {}

std::vector<MarketEvent> Simulator::poll() {
  if (!connected_ || finished_) {
    return {};
  }

  if (!pending_events_.empty()) {
    return drain_pending();
  }

  while (!schedule_.empty()) {
    const auto next_ts = schedule_.top().ts;
    if (next_ts > config_.simulation.duration_us) {
      finished_ = true;
      summary_.final_positions.clear();
      for (const auto& [symbol, position] : snapshot_.positions) {
        summary_.final_positions[symbol] = position.qty;
      }
      recompute_pnl();
      summary_.pnl = snapshot_.pnl;
      return {};
    }

    now_ = next_ts;
    snapshot_.now = now_;
    while (!schedule_.empty() && schedule_.top().ts == next_ts) {
      auto scheduled = schedule_.top();
      schedule_.pop();
      scheduled.fn();
    }
    if (!pending_events_.empty()) {
      return drain_pending();
    }
  }

  finished_ = true;
  return {};
}

void Simulator::submit(const std::vector<OrderCommand>& commands) {
  for (const auto& command : commands) {
    if (logger_) {
      logger_->log_order_command(now_, command);
    }
    process_command(command);
  }
}

void Simulator::initialize_books() {
  for (const auto symbol : all_symbols()) {
    schedule_maker_refresh(symbol, 0);
  }
}

void Simulator::schedule_initial_actions() {
  schedule_timer_tick(config_.simulation.strategy_timer_us);
  for (const auto symbol : all_symbols()) {
    schedule_noise_tick(symbol, config_.simulation.noise_interval_us);
  }
  schedule_event_wave(config_.simulation.event_interval_us);
  if (config_.simulation.enable_passive_opponent) {
    for (const auto symbol : all_symbols()) {
      schedule_passive_opponent_tick(symbol, 2'000);
    }
  }
  if (config_.simulation.enable_naive_arb_opponent) {
    schedule_naive_arb_tick(4'000);
  }
}

void Simulator::schedule_timer_tick(TimestampUs ts) {
  schedule(
      [this, ts]() {
        queue_event(Timer{ts, "strategy_timer"});
        schedule_timer_tick(ts + config_.simulation.strategy_timer_us);
      },
      ts);
}

void Simulator::schedule_noise_tick(Symbol symbol, TimestampUs ts) {
  schedule(
      [this, symbol, ts]() {
        if (ts <= config_.simulation.duration_us) {
          process_noise(symbol);
          std::uniform_int_distribution<int> jitter(0, 750);
          schedule_noise_tick(symbol, ts + config_.simulation.noise_interval_us + jitter(rng_));
        }
      },
      ts);
}

void Simulator::schedule_event_wave(TimestampUs ts) {
  schedule(
      [this, ts]() {
        if (ts > config_.simulation.duration_us) {
          return;
        }
        std::uniform_int_distribution<int> symbol_dist(0, 2);
        std::uniform_int_distribution<int> side_dist(0, 1);
        const auto symbol = std::array<Symbol, 3>{Symbol::E, Symbol::T, Symbol::F}[symbol_dist(rng_)];
        const auto side = side_dist(rng_) == 0 ? Side::Buy : Side::Sell;
        for (int i = 0; i < config_.simulation.event_burst_count; ++i) {
          schedule([this, symbol, side]() { process_event(symbol, side); }, ts + i * 500);
        }
        std::uniform_int_distribution<int> jitter(0, 5'000);
        schedule_event_wave(ts + config_.simulation.event_interval_us + jitter(rng_));
      },
      ts);
}

void Simulator::schedule_passive_opponent_tick(Symbol symbol, TimestampUs ts) {
  schedule(
      [this, symbol, ts]() {
        if (ts <= config_.simulation.duration_us) {
          refresh_passive_opponent(symbol);
          schedule_passive_opponent_tick(symbol, ts + 4'000);
        }
      },
      ts);
}

void Simulator::schedule_naive_arb_tick(TimestampUs ts) {
  schedule(
      [this, ts]() {
        if (ts <= config_.simulation.duration_us) {
          process_naive_arb();
          schedule_naive_arb_tick(ts + 5'000);
        }
      },
      ts);
}

void Simulator::schedule_maker_refresh(Symbol symbol, TimestampUs ts) {
  maker_ready_ts_[symbol] = ts;
  schedule(
      [this, symbol, ts]() {
        if (maker_ready_ts_[symbol] == ts) {
          refresh_maker_quotes(symbol);
        }
      },
      ts);
}

void Simulator::process_noise(Symbol symbol) {
  std::uniform_int_distribution<int> side_dist(0, 1);
  const auto side = side_dist(rng_) == 0 ? Side::Buy : Side::Sell;
  const auto capped = std::min(flow_size_for_width(symbol, config_.simulation.noise_base_qty *
                                                              config_.simulation.noise_bot_count),
                               displayed_best_qty(symbol, side == Side::Buy ? Side::Sell : Side::Buy));
  if (capped > 0) {
    aggress_symbol(symbol, side, capped, "noise_bot", false);
  }
}

void Simulator::process_event(Symbol symbol, Side side) {
  const auto capped = std::min(flow_size_for_width(symbol, config_.simulation.event_base_qty *
                                                              config_.simulation.event_bot_count),
                               displayed_best_qty(symbol, side == Side::Buy ? Side::Sell : Side::Buy));
  if (capped > 0) {
    aggress_symbol(symbol, side, capped, "event_bot", false);
  }
}

void Simulator::refresh_maker_quotes(Symbol symbol) {
  std::vector<OrderId> cancel_ids;
  for (const auto& [id, order] : orders_) {
    if (order.owner == owner_for_maker(symbol)) {
      cancel_ids.push_back(id);
    }
  }
  for (const auto id : cancel_ids) {
    remove_order(id);
  }

  const auto fair = maker_bot_fair(maker_position_[symbol], config_.simulation.maker_k);
  for (const auto offset : config_.simulation.maker_offsets) {
    const auto bid_price =
        validate_price_tick(static_cast<PriceTick>(std::floor(fair)) - offset, config_.simulation.price_min,
                            config_.simulation.price_max);
    const auto ask_price =
        validate_price_tick(static_cast<PriceTick>(std::ceil(fair)) + offset, config_.simulation.price_min,
                            config_.simulation.price_max);
    place_limit(internal_order_id_++, symbol, Side::Buy, bid_price, config_.simulation.maker_level_qty,
                "maker_quote", false);
    place_limit(internal_order_id_++, symbol, Side::Sell, ask_price, config_.simulation.maker_level_qty,
                "maker_quote", false);
  }
  emit_book_if_changed(symbol);
}

void Simulator::refresh_passive_opponent(Symbol symbol) {
  std::vector<OrderId> cancel_ids;
  for (const auto& [id, order] : orders_) {
    if (order.owner == owner_for_passive_opponent(symbol)) {
      cancel_ids.push_back(id);
    }
  }
  for (const auto id : cancel_ids) {
    remove_order(id);
  }

  const auto fair = mid_price(compute_top(symbol));
  const auto bid = validate_price_tick(static_cast<PriceTick>(std::floor(fair)) - 2, config_.simulation.price_min,
                                       config_.simulation.price_max);
  const auto ask = validate_price_tick(static_cast<PriceTick>(std::ceil(fair)) + 2, config_.simulation.price_min,
                                       config_.simulation.price_max);
  place_limit(internal_order_id_++, symbol, Side::Buy, bid, 4, "opp_passive_bid", false);
  orders_[internal_order_id_ - 1].owner = owner_for_passive_opponent(symbol);
  place_limit(internal_order_id_++, symbol, Side::Sell, ask, 4, "opp_passive_ask", false);
  orders_[internal_order_id_ - 1].owner = owner_for_passive_opponent(symbol);
  emit_book_if_changed(symbol);
}

void Simulator::process_naive_arb() {
  if (!best_order_id(Symbol::ETF, Side::Buy) || !best_order_id(Symbol::ETF, Side::Sell)) {
    return;
  }
  const auto top_etf = compute_top(Symbol::ETF);
  const auto top_e = compute_top(Symbol::E);
  const auto top_t = compute_top(Symbol::T);
  const auto top_f = compute_top(Symbol::F);
  if (!top_etf.bid || !top_etf.ask || !top_e.bid || !top_e.ask || !top_t.bid || !top_t.ask || !top_f.bid ||
      !top_f.ask) {
    return;
  }

  const auto rich_edge = 6 * top_etf.bid->price - (top_e.ask->price + 2 * top_t.ask->price + 3 * top_f.ask->price);
  const auto cheap_edge = (top_e.bid->price + 2 * top_t.bid->price + 3 * top_f.bid->price) - 6 * top_etf.ask->price;
  if (rich_edge >= config_.strategy.edge_buffer_ticks * 2) {
    aggress_symbol(Symbol::ETF, Side::Sell, 6, "opp_naive_arb", false);
    aggress_symbol(Symbol::E, Side::Buy, 1, "opp_naive_arb", false);
    aggress_symbol(Symbol::T, Side::Buy, 2, "opp_naive_arb", false);
    aggress_symbol(Symbol::F, Side::Buy, 3, "opp_naive_arb", false);
  } else if (cheap_edge >= config_.strategy.edge_buffer_ticks * 2) {
    aggress_symbol(Symbol::ETF, Side::Buy, 6, "opp_naive_arb", false);
    aggress_symbol(Symbol::E, Side::Sell, 1, "opp_naive_arb", false);
    aggress_symbol(Symbol::T, Side::Sell, 2, "opp_naive_arb", false);
    aggress_symbol(Symbol::F, Side::Sell, 3, "opp_naive_arb", false);
  }
}

void Simulator::queue_event(const MarketEvent& event) {
  pending_events_.push_back(event);
  if (logger_) {
    logger_->log_market_event(event);
  }
}

std::vector<MarketEvent> Simulator::drain_pending() {
  auto events = pending_events_;
  pending_events_.clear();
  return events;
}

void Simulator::emit_book_if_changed(Symbol symbol) {
  const auto top = compute_top(symbol);
  books_[symbol].current = top;
  snapshot_.books[symbol] = top;
  const auto& last = books_[symbol].last_emitted;
  if ((!last.bid && top.bid) || (!last.ask && top.ask) || (last.bid && top.bid &&
                                                            (last.bid->price != top.bid->price ||
                                                             last.bid->qty != top.bid->qty)) ||
      (last.ask && top.ask &&
       (last.ask->price != top.ask->price || last.ask->qty != top.ask->qty)) || (!!last.bid != !!top.bid) ||
      (!!last.ask != !!top.ask)) {
    books_[symbol].last_emitted = top;
    queue_event(BookUpdate{now_, top});
  }
}

void Simulator::process_command(const OrderCommand& command) {
  std::visit(
      [this](const auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, PlaceLimit>) {
          place_limit(payload.order_id, payload.symbol, payload.side, payload.price, payload.qty, payload.tag, true);
        } else if constexpr (std::is_same_v<T, Cancel>) {
          cancel_order(payload.order_id);
        } else {
          cancel_replace(payload.order_id, payload.new_price, payload.new_qty, payload.tag);
        }
      },
      command);
}

void Simulator::place_limit(OrderId order_id, Symbol symbol, Side side, PriceTick price, Qty qty,
                            const std::string& tag, bool team_owned) {
  try {
    validate_price_tick(price, config_.simulation.price_min, config_.simulation.price_max);
  } catch (const std::exception& ex) {
    if (team_owned) {
      queue_event(Reject{now_, order_id, symbol, ex.what()});
    }
    return;
  }

  const auto top = compute_top(symbol);
  const auto marketable =
      (side == Side::Buy && top.ask && price >= top.ask->price) || (side == Side::Sell && top.bid && price <= top.bid->price);
  if (team_owned && !marketable) {
    std::size_t active_for_symbol = 0;
    for (const auto& [id, order] : orders_) {
      if (order.team_owned && order.symbol == symbol) {
        ++active_for_symbol;
      }
    }
    try {
      validate_active_order_limit(active_for_symbol);
    } catch (const std::exception& ex) {
      queue_event(Reject{now_, order_id, symbol, ex.what()});
      return;
    }
  }

  if (team_owned) {
    queue_event(Ack{now_, order_id, symbol, side, price, qty, tag});
  }

  RestingOrder incoming{
      order_id,
      team_owned ? "team" : "system",
      symbol,
      side,
      price,
      qty,
      now_,
      team_owned,
      false,
      tag,
  };

  const auto source = tag.empty() ? (team_owned ? "team" : "system") : tag;
  const auto passive_side = side == Side::Buy ? Side::Sell : Side::Buy;

  while (incoming.qty > 0) {
    const auto resting_id = best_order_id(symbol, passive_side);
    if (!resting_id) {
      break;
    }
    auto& resting = orders_.at(*resting_id);
    const auto crosses = side == Side::Buy ? incoming.price >= resting.price : incoming.price <= resting.price;
    if (!crosses) {
      break;
    }

    const auto trade_qty = std::min(incoming.qty, resting.qty);
    const auto trade_price = resting.price;
    const auto team_involved = team_owned || resting.team_owned;
    queue_event(TradePrint{now_, symbol, side, trade_price, trade_qty, source, resting.id, team_involved});

    if (team_owned) {
      Fill fill{now_, symbol, incoming.id, side, trade_price, trade_qty, true, source, incoming.tag};
      queue_event(fill);
      update_team_position(fill);
    }
    if (resting.team_owned) {
      Fill fill{now_, symbol, resting.id, resting.side, trade_price, trade_qty, false, source, resting.tag};
      queue_event(fill);
      update_team_position(fill);
    }

    if (resting.owner == owner_for_maker(symbol)) {
      update_owner_position(resting.owner, resting.side, trade_qty);
      schedule_maker_refresh(symbol, now_ + config_.simulation.maker_reload_us);
    }
    if (source == "noise_bot") {
      noise_positions_[symbol] += side == Side::Buy ? trade_qty : -trade_qty;
    }
    if (source == "event_bot") {
      event_positions_[symbol] += side == Side::Buy ? trade_qty : -trade_qty;
    }

    resting.qty -= trade_qty;
    incoming.qty -= trade_qty;
    if (resting.qty <= 0) {
      remove_order(resting.id);
    }
  }

  if (incoming.qty > 0) {
    insert_order(incoming);
  }
  emit_book_if_changed(symbol);
  recompute_pnl();
}

void Simulator::cancel_order(OrderId order_id) {
  auto it = orders_.find(order_id);
  if (it == orders_.end()) {
    return;
  }
  const auto symbol = it->second.symbol;
  const auto team_owned = it->second.team_owned;
  remove_order(order_id);
  if (team_owned) {
    queue_event(CancelAck{now_, order_id, symbol});
  }
  emit_book_if_changed(symbol);
}

void Simulator::cancel_replace(OrderId order_id, PriceTick new_price, Qty new_qty, const std::string& tag) {
  auto it = orders_.find(order_id);
  if (it == orders_.end()) {
    return;
  }
  const auto symbol = it->second.symbol;
  const auto side = it->second.side;
  cancel_order(order_id);
  place_limit(order_id, symbol, side, new_price, new_qty, tag, true);
}

void Simulator::aggress_symbol(Symbol symbol, Side side, Qty qty, const std::string& source, bool team_owned,
                               std::optional<OrderId> order_id, const std::string& tag) {
  if (qty <= 0) {
    return;
  }
  const auto price = side == Side::Buy ? config_.simulation.price_max : config_.simulation.price_min;
  place_limit(order_id.value_or(internal_order_id_++), symbol, side, price, qty, tag.empty() ? source : tag,
              team_owned);
}

void Simulator::insert_order(const RestingOrder& order) {
  orders_[order.id] = order;
  if (order.side == Side::Buy) {
    bids_[order.symbol][order.price].push_back(order.id);
  } else {
    asks_[order.symbol][order.price].push_back(order.id);
  }
  if (!order.team_owned && order.tag == "opp_passive_bid") {
    orders_[order.id].owner = owner_for_passive_opponent(order.symbol);
    orders_[order.id].opponent_owned = true;
  }
  if (!order.team_owned && order.tag == "opp_passive_ask") {
    orders_[order.id].owner = owner_for_passive_opponent(order.symbol);
    orders_[order.id].opponent_owned = true;
  }
  if (!order.team_owned && order.tag == "maker_quote") {
    orders_[order.id].owner = owner_for_maker(order.symbol);
  }
}

void Simulator::remove_order(OrderId order_id) {
  auto it = orders_.find(order_id);
  if (it == orders_.end()) {
    return;
  }
  const auto symbol = it->second.symbol;
  const auto price = it->second.price;
  if (it->second.side == Side::Buy) {
    auto& side_book = bids_[symbol][price];
    side_book.erase(std::remove(side_book.begin(), side_book.end(), order_id), side_book.end());
    if (side_book.empty()) {
      bids_[symbol].erase(price);
    }
  } else {
    auto& side_book = asks_[symbol][price];
    side_book.erase(std::remove(side_book.begin(), side_book.end(), order_id), side_book.end());
    if (side_book.empty()) {
      asks_[symbol].erase(price);
    }
  }
  orders_.erase(it);
}

std::optional<OrderId> Simulator::best_order_id(Symbol symbol, Side side) const {
  if (side == Side::Buy) {
    const auto book_it = bids_.find(symbol);
    if (book_it == bids_.end() || book_it->second.empty()) {
      return std::nullopt;
    }
    const auto& ids = book_it->second.begin()->second;
    if (ids.empty()) {
      return std::nullopt;
    }
    return ids.front();
  }
  const auto book_it = asks_.find(symbol);
  if (book_it == asks_.end() || book_it->second.empty()) {
    return std::nullopt;
  }
  const auto& ids = book_it->second.begin()->second;
  if (ids.empty()) {
    return std::nullopt;
  }
  return ids.front();
}

TopOfBook Simulator::compute_top(Symbol symbol) const {
  TopOfBook book;
  book.symbol = symbol;
  const auto bid_it = bids_.find(symbol);
  if (bid_it != bids_.end() && !bid_it->second.empty()) {
    Qty total_qty = 0;
    for (const auto id : bid_it->second.begin()->second) {
      total_qty += orders_.at(id).qty;
    }
    book.bid = BookLevel{bid_it->second.begin()->first, total_qty};
  }
  const auto ask_it = asks_.find(symbol);
  if (ask_it != asks_.end() && !ask_it->second.empty()) {
    Qty total_qty = 0;
    for (const auto id : ask_it->second.begin()->second) {
      total_qty += orders_.at(id).qty;
    }
    book.ask = BookLevel{ask_it->second.begin()->first, total_qty};
  }

  if (!book.bid || !book.ask) {
    const auto fallback = config_.simulation.initial_books.at(symbol);
    if (!book.bid) {
      book.bid = fallback.bid;
    }
    if (!book.ask) {
      book.ask = fallback.ask;
    }
  }
  return book;
}

Qty Simulator::displayed_best_qty(Symbol symbol, Side side) const {
  const auto top = compute_top(symbol);
  if (side == Side::Buy) {
    return top.bid ? top.bid->qty : 0;
  }
  return top.ask ? top.ask->qty : 0;
}

Qty Simulator::flow_size_for_width(Symbol symbol, Qty base_qty) const {
  return width_scaled_flow(book_width(symbol), base_qty);
}

double Simulator::book_width(Symbol symbol) const {
  const auto top = compute_top(symbol);
  if (!top.bid || !top.ask) {
    return 4.0;
  }
  return static_cast<double>(top.ask->price - top.bid->price);
}

void Simulator::update_team_position(const Fill& fill) {
  apply_fill(snapshot_.positions[fill.symbol], fill.side, fill.price, fill.qty);
  if (is_team_tag(fill.tag, "basket_arb")) {
    summary_.arb_fills += 1;
    summary_.fill_breakdown["basket_arb"] += fill.qty;
  } else if (is_team_tag(fill.tag, "event_reprice")) {
    summary_.fill_breakdown["event_reprice"] += fill.qty;
  } else if (is_team_tag(fill.tag, "quote")) {
    summary_.fill_breakdown["quotes"] += fill.qty;
  } else {
    summary_.fill_breakdown["other"] += fill.qty;
  }
}

void Simulator::update_owner_position(const std::string& owner_key, Side passive_side, Qty qty) {
  if (owner_key.rfind("maker:", 0) == 0) {
    const auto symbol = symbol_from_string(owner_key.substr(6));
    maker_position_[symbol] += passive_side == Side::Buy ? qty : -qty;
  }
}

void Simulator::recompute_pnl() {
  snapshot_.pnl = {};
  for (const auto symbol : all_symbols()) {
    const auto mark = mid_price(compute_top(symbol));
    snapshot_.pnl.realized += snapshot_.positions[symbol].realized;
    snapshot_.pnl.unrealized += unrealized_for_position(snapshot_.positions[symbol], mark);
  }
  snapshot_.pnl.total = snapshot_.pnl.realized + snapshot_.pnl.unrealized;
}

void Simulator::schedule(std::function<void()> fn, TimestampUs ts) {
  schedule_.push(ScheduledAction{ts, sequence_++, std::move(fn)});
}

}  // namespace etf
