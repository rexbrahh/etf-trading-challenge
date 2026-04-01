#include "etf/strategy.hpp"

#include <algorithm>
#include <cmath>
#include <deque>
#include <memory>
#include <string_view>
#include <stdexcept>

namespace etf {

namespace {

struct EventSignal {
  Symbol symbol{Symbol::E};
  int signed_qty{0};
  bool active{false};
};

class StrategyBase : public Strategy {
 public:
  explicit StrategyBase(StrategyConfig config) : config_(std::move(config)) {}

 protected:
  void sync_from_snapshot(const MarketSnapshot& snapshot) {
    books_ = snapshot.books;
  }

  void ingest_event(const MarketEvent& event) {
    std::visit(
        [this](const auto& payload) {
          using T = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<T, BookUpdate>) {
            books_[payload.book.symbol] = payload.book;
          } else if constexpr (std::is_same_v<T, TradePrint>) {
            auto& trades = recent_trades_[payload.symbol];
            trades.push_back(payload);
            while (!trades.empty() && payload.ts - trades.front().ts > config_.event_lookback_us) {
              trades.pop_front();
            }
            if ((payload.source == "noise_bot" || payload.source == "event_bot") && !payload.team_involved &&
                payload.symbol != Symbol::ETF) {
              const auto passive_side =
                  payload.aggressor_side == Side::Buy ? Side::Sell : Side::Buy;
              maker_flow_estimate_[payload.symbol] += passive_side == Side::Buy ? payload.qty : -payload.qty;
              const auto mid = mid_price(books_.at(payload.symbol));
              const auto deviation = mid - 500.0;
              if (std::abs(deviation) > 0.1) {
                k_estimate_[payload.symbol] =
                    std::clamp(std::abs(maker_flow_estimate_[payload.symbol] / deviation), 1.0, 99.0);
              }
            }
          } else if constexpr (std::is_same_v<T, Ack>) {
            active_orders_[payload.order_id] = OrderView{
                payload.order_id, payload.symbol, payload.side, payload.price, payload.qty,
                OrderState::Active, payload.tag};
          } else if constexpr (std::is_same_v<T, Fill>) {
            auto it = active_orders_.find(payload.order_id);
            if (it != active_orders_.end()) {
              it->second.remaining_qty -= payload.qty;
              it->second.state = it->second.remaining_qty > 0 ? OrderState::PartiallyFilled : OrderState::Filled;
              if (it->second.remaining_qty <= 0) {
                active_orders_.erase(it);
              }
            }
          } else if constexpr (std::is_same_v<T, CancelAck>) {
            active_orders_.erase(payload.order_id);
          } else if constexpr (std::is_same_v<T, Reject>) {
            if (payload.order_id) {
              active_orders_.erase(*payload.order_id);
            }
          }
        },
        event);
  }

  OrderId next_order_id() { return next_order_id_++; }

  std::optional<PriceTick> best_bid(Symbol symbol) const {
    const auto it = books_.find(symbol);
    if (it == books_.end() || !it->second.bid) {
      return std::nullopt;
    }
    return it->second.bid->price;
  }

  std::optional<PriceTick> best_ask(Symbol symbol) const {
    const auto it = books_.find(symbol);
    if (it == books_.end() || !it->second.ask) {
      return std::nullopt;
    }
    return it->second.ask->price;
  }

  bool within_risk(const MarketSnapshot& snapshot, Symbol symbol, int delta) const {
    const auto current = snapshot.positions.contains(symbol) ? snapshot.positions.at(symbol).qty : 0;
    return std::abs(current + delta) <= config_.max_abs_position;
  }

  bool has_active_tag(std::string_view needle) const {
    return std::any_of(active_orders_.begin(), active_orders_.end(), [&](const auto& entry) {
      return entry.second.tag.find(needle) != std::string::npos;
    });
  }

  bool has_active_order(Symbol symbol, Side side, std::string_view needle) const {
    return std::any_of(active_orders_.begin(), active_orders_.end(), [&](const auto& entry) {
      return entry.second.symbol == symbol && entry.second.side == side &&
             entry.second.tag.find(needle) != std::string::npos;
    });
  }

  StrategyDecision quote_etf(const MarketSnapshot& snapshot, double fair, const std::string& prefix) {
    StrategyDecision decision;
    const auto bid_price = validate_price_tick(static_cast<PriceTick>(std::floor(fair) - config_.quote_width_ticks));
    const auto ask_price = validate_price_tick(static_cast<PriceTick>(std::ceil(fair) + config_.quote_width_ticks));

    if (within_risk(snapshot, Symbol::ETF, config_.aggressive_qty)) {
      if (bid_quote_id_ && active_orders_.contains(*bid_quote_id_)) {
        auto& order = active_orders_.at(*bid_quote_id_);
        order.price = bid_price;
        order.remaining_qty = config_.aggressive_qty;
        order.state = OrderState::PendingNew;
        order.tag = prefix + "_bid_quote";
        decision.commands.push_back(
            CancelReplace{*bid_quote_id_, bid_price, config_.aggressive_qty, prefix + "_bid_quote"});
      } else {
        const auto id = next_order_id();
        bid_quote_id_ = id;
        active_orders_[id] = OrderView{id, Symbol::ETF, Side::Buy, bid_price, config_.aggressive_qty,
                                       OrderState::PendingNew, prefix + "_bid_quote"};
        decision.commands.push_back(
            PlaceLimit{id, Symbol::ETF, Side::Buy, bid_price, config_.aggressive_qty, prefix + "_bid_quote"});
      }
    }
    if (within_risk(snapshot, Symbol::ETF, -config_.aggressive_qty)) {
      if (ask_quote_id_ && active_orders_.contains(*ask_quote_id_)) {
        auto& order = active_orders_.at(*ask_quote_id_);
        order.price = ask_price;
        order.remaining_qty = config_.aggressive_qty;
        order.state = OrderState::PendingNew;
        order.tag = prefix + "_ask_quote";
        decision.commands.push_back(
            CancelReplace{*ask_quote_id_, ask_price, config_.aggressive_qty, prefix + "_ask_quote"});
      } else {
        const auto id = next_order_id();
        ask_quote_id_ = id;
        active_orders_[id] = OrderView{id, Symbol::ETF, Side::Sell, ask_price, config_.aggressive_qty,
                                       OrderState::PendingNew, prefix + "_ask_quote"};
        decision.commands.push_back(
            PlaceLimit{id, Symbol::ETF, Side::Sell, ask_price, config_.aggressive_qty, prefix + "_ask_quote"});
      }
    }
    decision.diagnostics["quoted_fair"] = fair;
    return decision;
  }

  void maybe_cancel_stale_quote(Side stale_side, StrategyDecision& decision) {
    const auto target_id = stale_side == Side::Buy ? bid_quote_id_ : ask_quote_id_;
    if (target_id && active_orders_.contains(*target_id)) {
      decision.commands.push_back(Cancel{*target_id, "stale_cancel"});
      stale_quote_marks_[*target_id] = true;
      active_orders_.erase(*target_id);
      if (stale_side == Side::Buy) {
        bid_quote_id_.reset();
      } else {
        ask_quote_id_.reset();
      }
    }
  }

  EventSignal detect_event_signal(Symbol symbol, TimestampUs now) const {
    EventSignal signal{symbol, 0, false};
    const auto trades_it = recent_trades_.find(symbol);
    if (trades_it == recent_trades_.end()) {
      return signal;
    }
    int signed_qty = 0;
    for (const auto& trade : trades_it->second) {
      if (now - trade.ts <= config_.event_lookback_us) {
        signed_qty += trade.aggressor_side == Side::Buy ? trade.qty : -trade.qty;
      }
    }
    signal.signed_qty = signed_qty;
    signal.active = std::abs(signed_qty) >= config_.event_threshold_qty;
    return signal;
  }

  double implied_etf_fair() const { return synthetic_etf_fair(books_); }

  StrategyConfig config_;
  std::map<Symbol, TopOfBook> books_;
  std::unordered_map<OrderId, OrderView> active_orders_;
  std::unordered_map<OrderId, bool> stale_quote_marks_;
  std::map<Symbol, std::deque<TradePrint>> recent_trades_;
  std::map<Symbol, double> maker_flow_estimate_;
  std::map<Symbol, double> k_estimate_;
  std::optional<OrderId> bid_quote_id_;
  std::optional<OrderId> ask_quote_id_;
  OrderId next_order_id_{1};
};

StrategyDecision merge(StrategyDecision lhs, StrategyDecision rhs) {
  lhs.commands.insert(lhs.commands.end(), rhs.commands.begin(), rhs.commands.end());
  for (auto it = rhs.diagnostics.begin(); it != rhs.diagnostics.end(); ++it) {
    lhs.diagnostics[it.key()] = it.value();
  }
  return lhs;
}

class BasketArbStrategy final : public StrategyBase {
 public:
  using StrategyBase::StrategyBase;

  std::string name() const override { return "basket_arb"; }

  StrategyDecision on_start(const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return {};
  }

  StrategyDecision on_event(const MarketEvent& event, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    ingest_event(event);
    return {};
  }

  StrategyDecision on_timer(const Timer&, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return maybe_trade(snapshot);
  }

  StrategyDecision on_end(const MarketSnapshot&) override { return {}; }

 private:
  StrategyDecision maybe_trade(const MarketSnapshot& snapshot) {
    if (!best_bid(Symbol::ETF) || !best_ask(Symbol::ETF) || !best_bid(Symbol::E) || !best_bid(Symbol::T) ||
        !best_bid(Symbol::F) || !best_ask(Symbol::E) || !best_ask(Symbol::T) || !best_ask(Symbol::F)) {
      return {};
    }

    StrategyDecision decision;
    const auto rich_edge =
        6 * best_bid(Symbol::ETF).value() -
        (best_ask(Symbol::E).value() + 2 * best_ask(Symbol::T).value() + 3 * best_ask(Symbol::F).value());
    const auto cheap_edge =
        (best_bid(Symbol::E).value() + 2 * best_bid(Symbol::T).value() + 3 * best_bid(Symbol::F).value()) -
        6 * best_ask(Symbol::ETF).value();

    decision.diagnostics["rich_edge"] = rich_edge;
    decision.diagnostics["cheap_edge"] = cheap_edge;

    if (rich_edge >= config_.edge_buffer_ticks && within_risk(snapshot, Symbol::ETF, -6 * config_.hedge_unit) &&
        !has_active_tag("basket_arb")) {
      const auto hedge = config_.hedge_unit;
      decision.commands.push_back(
          PlaceLimit{next_order_id(), Symbol::ETF, Side::Sell, best_bid(Symbol::ETF).value(), 6 * hedge,
                     "basket_arb_sell_etf"});
      if (config_.hedge_behavior) {
        if (within_risk(snapshot, Symbol::E, hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::E, Side::Buy, best_ask(Symbol::E).value(), hedge,
                         "basket_arb_buy_e"});
        }
        if (within_risk(snapshot, Symbol::T, 2 * hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::T, Side::Buy, best_ask(Symbol::T).value(), 2 * hedge,
                         "basket_arb_buy_t"});
        }
        if (within_risk(snapshot, Symbol::F, 3 * hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::F, Side::Buy, best_ask(Symbol::F).value(), 3 * hedge,
                         "basket_arb_buy_f"});
        }
      }
    } else if (cheap_edge >= config_.edge_buffer_ticks &&
               within_risk(snapshot, Symbol::ETF, 6 * config_.hedge_unit) && !has_active_tag("basket_arb")) {
      const auto hedge = config_.hedge_unit;
      decision.commands.push_back(
          PlaceLimit{next_order_id(), Symbol::ETF, Side::Buy, best_ask(Symbol::ETF).value(), 6 * hedge,
                     "basket_arb_buy_etf"});
      if (config_.hedge_behavior) {
        if (within_risk(snapshot, Symbol::E, -hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::E, Side::Sell, best_bid(Symbol::E).value(), hedge,
                         "basket_arb_sell_e"});
        }
        if (within_risk(snapshot, Symbol::T, -2 * hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::T, Side::Sell, best_bid(Symbol::T).value(), 2 * hedge,
                         "basket_arb_sell_t"});
        }
        if (within_risk(snapshot, Symbol::F, -3 * hedge)) {
          decision.commands.push_back(
              PlaceLimit{next_order_id(), Symbol::F, Side::Sell, best_bid(Symbol::F).value(), 3 * hedge,
                         "basket_arb_sell_f"});
        }
      }
    }
    return decision;
  }
};

class EventPropStrategy final : public StrategyBase {
 public:
  using StrategyBase::StrategyBase;

  std::string name() const override { return "event_prop"; }

  StrategyDecision on_start(const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    if (books_.size() == 4) {
      return quote_etf(snapshot, implied_etf_fair(), "event_prop");
    }
    return {};
  }

  StrategyDecision on_event(const MarketEvent& event, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    ingest_event(event);
    StrategyDecision decision;

    if (std::holds_alternative<TradePrint>(event) || std::holds_alternative<Fill>(event)) {
      const auto fair = implied_etf_fair();
      auto signal = strongest_signal(snapshot.now);
      decision.diagnostics["implied_etf"] = fair;
      decision.diagnostics["signal_symbol"] = signal.active ? to_string(signal.symbol) : "none";
      decision.diagnostics["signal_signed_qty"] = signal.signed_qty;

      if (signal.active && within_risk(snapshot, Symbol::ETF,
                                       signal.signed_qty > 0 ? config_.aggressive_qty : -config_.aggressive_qty) &&
          !has_active_order(Symbol::ETF, signal.signed_qty > 0 ? Side::Buy : Side::Sell, "event_reprice")) {
        maybe_cancel_stale_quote(signal.signed_qty > 0 ? Side::Sell : Side::Buy, decision);
        const auto aggressive_side = signal.signed_qty > 0 ? Side::Buy : Side::Sell;
        const auto price = aggressive_side == Side::Buy ? best_ask(Symbol::ETF).value()
                                                        : best_bid(Symbol::ETF).value();
        decision.commands.push_back(PlaceLimit{
            next_order_id(), Symbol::ETF, aggressive_side, price, config_.aggressive_qty,
            aggressive_side == Side::Buy ? "event_reprice_buy" : "event_reprice_sell"});
      } else {
        decision = merge(std::move(decision), quote_etf(snapshot, fair, "event_prop"));
      }
    }
    return decision;
  }

  StrategyDecision on_timer(const Timer&, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    if (books_.size() == 4) {
      return quote_etf(snapshot, implied_etf_fair(), "event_prop");
    }
    return {};
  }

  StrategyDecision on_end(const MarketSnapshot&) override { return {}; }

 private:
  EventSignal strongest_signal(TimestampUs now) const {
    EventSignal best;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      const auto candidate = detect_event_signal(symbol, now);
      if (candidate.active && std::abs(candidate.signed_qty) > std::abs(best.signed_qty)) {
        best = candidate;
      }
    }
    return best;
  }
};

class HybridStrategy final : public StrategyBase {
 public:
  using StrategyBase::StrategyBase;

  std::string name() const override { return "hybrid"; }

  StrategyDecision on_start(const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return quote_etf(snapshot, implied_etf_fair(), "hybrid");
  }

  StrategyDecision on_event(const MarketEvent& event, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    ingest_event(event);
    if (std::holds_alternative<TradePrint>(event) || std::holds_alternative<Fill>(event)) {
      auto decision = react_to_signal(snapshot);
      decision.diagnostics["k_estimate_e"] =
          k_estimate_.contains(Symbol::E) ? nlohmann::json(k_estimate_.at(Symbol::E)) : nlohmann::json(nullptr);
      decision.diagnostics["k_estimate_t"] =
          k_estimate_.contains(Symbol::T) ? nlohmann::json(k_estimate_.at(Symbol::T)) : nlohmann::json(nullptr);
      decision.diagnostics["k_estimate_f"] =
          k_estimate_.contains(Symbol::F) ? nlohmann::json(k_estimate_.at(Symbol::F)) : nlohmann::json(nullptr);
      return decision;
    }
    return {};
  }

  StrategyDecision on_timer(const Timer&, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return evaluate(snapshot);
  }

  StrategyDecision on_end(const MarketSnapshot&) override { return {}; }

 private:
  StrategyDecision react_to_signal(const MarketSnapshot& snapshot) {
    StrategyDecision decision;
    const auto signal = strongest_signal(snapshot.now);
    decision.diagnostics["signal_signed_qty"] = signal.signed_qty;
    decision.diagnostics["signal_symbol"] = signal.active ? to_string(signal.symbol) : "none";
    decision.diagnostics["implied_etf"] = implied_etf_fair();

    if (signal.active && best_bid(Symbol::ETF) && best_ask(Symbol::ETF)) {
      maybe_cancel_stale_quote(signal.signed_qty > 0 ? Side::Sell : Side::Buy, decision);
      const auto side = signal.signed_qty > 0 ? Side::Buy : Side::Sell;
      const auto delta = side == Side::Buy ? config_.aggressive_qty : -config_.aggressive_qty;
      if (within_risk(snapshot, Symbol::ETF, delta) && !has_active_order(Symbol::ETF, side, "event_reprice")) {
        decision.commands.push_back(PlaceLimit{
            next_order_id(), Symbol::ETF, side,
            side == Side::Buy ? best_ask(Symbol::ETF).value() : best_bid(Symbol::ETF).value(),
            config_.aggressive_qty, side == Side::Buy ? "event_reprice_buy" : "event_reprice_sell"});
      }
    }
    return decision;
  }

  StrategyDecision evaluate(const MarketSnapshot& snapshot) {
    auto decision = react_to_signal(snapshot);
    if (!decision.diagnostics.at("signal_signed_qty").get<int>()) {
      decision = merge(std::move(decision), quote_etf(snapshot, implied_etf_fair(), "hybrid"));
    }

    if (best_bid(Symbol::ETF) && best_ask(Symbol::ETF) && best_bid(Symbol::E) && best_bid(Symbol::T) &&
        best_bid(Symbol::F) && best_ask(Symbol::E) && best_ask(Symbol::T) && best_ask(Symbol::F)) {
      const auto rich_edge =
          6 * best_bid(Symbol::ETF).value() -
          (best_ask(Symbol::E).value() + 2 * best_ask(Symbol::T).value() + 3 * best_ask(Symbol::F).value());
      const auto cheap_edge =
          (best_bid(Symbol::E).value() + 2 * best_bid(Symbol::T).value() + 3 * best_bid(Symbol::F).value()) -
          6 * best_ask(Symbol::ETF).value();
      decision.diagnostics["rich_edge"] = rich_edge;
      decision.diagnostics["cheap_edge"] = cheap_edge;

      if (rich_edge >= config_.edge_buffer_ticks && within_risk(snapshot, Symbol::ETF, -6 * config_.hedge_unit) &&
          !has_active_tag("basket_arb")) {
        const auto hedge = config_.hedge_unit;
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::ETF, Side::Sell, best_bid(Symbol::ETF).value(), 6 * hedge,
                       "basket_arb_sell_etf"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::E, Side::Buy, best_ask(Symbol::E).value(), hedge,
                       "basket_arb_buy_e"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::T, Side::Buy, best_ask(Symbol::T).value(), 2 * hedge,
                       "basket_arb_buy_t"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::F, Side::Buy, best_ask(Symbol::F).value(), 3 * hedge,
                       "basket_arb_buy_f"});
      } else if (cheap_edge >= config_.edge_buffer_ticks &&
                 within_risk(snapshot, Symbol::ETF, 6 * config_.hedge_unit) && !has_active_tag("basket_arb")) {
        const auto hedge = config_.hedge_unit;
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::ETF, Side::Buy, best_ask(Symbol::ETF).value(), 6 * hedge,
                       "basket_arb_buy_etf"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::E, Side::Sell, best_bid(Symbol::E).value(), hedge,
                       "basket_arb_sell_e"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::T, Side::Sell, best_bid(Symbol::T).value(), 2 * hedge,
                       "basket_arb_sell_t"});
        decision.commands.push_back(
            PlaceLimit{next_order_id(), Symbol::F, Side::Sell, best_bid(Symbol::F).value(), 3 * hedge,
                       "basket_arb_sell_f"});
      }
    }

    return decision;
  }

  EventSignal strongest_signal(TimestampUs now) const {
    EventSignal best;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      const auto candidate = detect_event_signal(symbol, now);
      if (candidate.active && std::abs(candidate.signed_qty) > std::abs(best.signed_qty)) {
        best = candidate;
      }
    }
    return best;
  }
};

}  // namespace

std::unique_ptr<Strategy> make_strategy(const StrategyConfig& config) {
  if (config.name == "basket_arb") {
    return std::make_unique<BasketArbStrategy>(config);
  }
  if (config.name == "event_prop") {
    return std::make_unique<EventPropStrategy>(config);
  }
  if (config.name == "hybrid") {
    return std::make_unique<HybridStrategy>(config);
  }
  throw std::invalid_argument("unknown strategy: " + config.name);
}

}  // namespace etf
