#include "etf/strategy.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace etf {

namespace {

struct EventSignal {
  Symbol symbol{Symbol::E};
  int signed_qty{0};
  bool active{false};
};

int symbol_weight(Symbol symbol) {
  switch (symbol) {
    case Symbol::E:
      return 1;
    case Symbol::T:
      return 2;
    case Symbol::F:
      return 3;
    case Symbol::ETF:
      return 6;
  }
  throw std::invalid_argument("unknown symbol weight");
}

class StrategyBase : public Strategy {
 public:
  explicit StrategyBase(StrategyConfig config) : config_(std::move(config)) {}

 protected:
  void sync_from_snapshot(const MarketSnapshot& snapshot) { books_ = snapshot.books; }

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
              const auto aggressor_signed_qty = payload.aggressor_side == Side::Buy ? payload.qty : -payload.qty;
              exo_flow_estimate_[payload.symbol] += aggressor_signed_qty;

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

  int active_orders_for_symbol(Symbol symbol) const {
    return static_cast<int>(std::count_if(active_orders_.begin(), active_orders_.end(), [&](const auto& entry) {
      return entry.second.symbol == symbol;
    }));
  }

  std::optional<PriceTick> passive_quote_price(Symbol symbol, Side side, double raw_price) const {
    PriceTick price = side == Side::Buy ? static_cast<PriceTick>(std::floor(raw_price))
                                        : static_cast<PriceTick>(std::ceil(raw_price));
    price = std::clamp(price, kMinPrice, kMaxPrice);
    const auto opposite = side == Side::Buy ? best_ask(symbol) : best_bid(symbol);
    if (opposite && ((side == Side::Buy && price >= *opposite) || (side == Side::Sell && price <= *opposite))) {
      return std::nullopt;
    }
    return price;
  }

  StrategyDecision quote_etf(const MarketSnapshot& snapshot, double fair, const std::string& prefix) {
    StrategyDecision decision;
    const auto bid_tag = prefix + "_bid_quote";
    const auto ask_tag = prefix + "_ask_quote";
    const auto bid_price = passive_quote_price(Symbol::ETF, Side::Buy, fair - config_.quote_width_ticks);
    const auto ask_price = passive_quote_price(Symbol::ETF, Side::Sell, fair + config_.quote_width_ticks);

    const auto refresh_side = [&](Side side, std::optional<OrderId>& slot, const std::optional<PriceTick>& price,
                                  const std::string& tag) {
      const auto qty = config_.aggressive_qty;
      const auto delta = side == Side::Buy ? qty : -qty;
      if (!price || !within_risk(snapshot, Symbol::ETF, delta)) {
        if (slot && active_orders_.contains(*slot)) {
          decision.commands.push_back(Cancel{*slot, "stale_cancel"});
          stale_quote_marks_[*slot] = true;
          active_orders_.erase(*slot);
          slot.reset();
        }
        return;
      }

      if (slot && active_orders_.contains(*slot)) {
        auto& order = active_orders_.at(*slot);
        if (order.price == *price && order.remaining_qty == qty && order.tag == tag) {
          return;
        }
        order.price = *price;
        order.remaining_qty = qty;
        order.state = OrderState::PendingNew;
        order.tag = tag;
        decision.commands.push_back(CancelReplace{*slot, *price, qty, tag});
        return;
      }

      const auto id = next_order_id();
      slot = id;
      active_orders_[id] =
          OrderView{id, Symbol::ETF, side, *price, qty, OrderState::PendingNew, tag};
      decision.commands.push_back(PlaceLimit{id, Symbol::ETF, side, *price, qty, tag});
    };

    refresh_side(Side::Buy, bid_quote_id_, bid_price, bid_tag);
    refresh_side(Side::Sell, ask_quote_id_, ask_price, ask_tag);
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
  std::map<Symbol, Qty> exo_flow_estimate_;
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
      const auto signal = strongest_signal(snapshot.now);
      decision.diagnostics["implied_etf"] = fair;
      decision.diagnostics["signal_symbol"] = signal.active ? to_string(signal.symbol) : "none";
      decision.diagnostics["signal_signed_qty"] = signal.signed_qty;

      if (signal.active &&
          within_risk(snapshot, Symbol::ETF, signal.signed_qty > 0 ? config_.aggressive_qty : -config_.aggressive_qty) &&
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

class ChallengeV1Strategy final : public StrategyBase {
 public:
  using StrategyBase::StrategyBase;

  std::string name() const override { return "challenge_v1"; }

  StrategyDecision on_start(const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return evaluate(snapshot);
  }

  StrategyDecision on_event(const MarketEvent& event, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    ingest_event(event);
    if (std::holds_alternative<TradePrint>(event) || std::holds_alternative<Fill>(event) ||
        std::holds_alternative<CancelAck>(event) || std::holds_alternative<Reject>(event)) {
      return react_to_market(snapshot);
    }
    return {};
  }

  StrategyDecision on_timer(const Timer&, const MarketSnapshot& snapshot) override {
    sync_from_snapshot(snapshot);
    return evaluate(snapshot);
  }

  StrategyDecision on_end(const MarketSnapshot&) override { return {}; }

 private:
  enum class OrderBucket { Quote, Hedge, Event };

  OrderBucket bucket_for_tag(const std::string& tag) const {
    if (tag.find("event_reprice") != std::string::npos) {
      return OrderBucket::Event;
    }
    if (tag.find("quote") != std::string::npos) {
      return OrderBucket::Quote;
    }
    return OrderBucket::Hedge;
  }

  int active_count(Symbol symbol, std::optional<OrderBucket> bucket = std::nullopt,
                   std::optional<OrderId> ignore = std::nullopt) const {
    return static_cast<int>(std::count_if(active_orders_.begin(), active_orders_.end(), [&](const auto& entry) {
      if (entry.second.symbol != symbol) {
        return false;
      }
      if (ignore && entry.first == *ignore) {
        return false;
      }
      if (bucket) {
        return bucket_for_tag(entry.second.tag) == *bucket;
      }
      return true;
    }));
  }

  bool has_capacity(Symbol symbol, OrderBucket bucket, std::optional<OrderId> ignore = std::nullopt) const {
    return active_count(symbol, std::nullopt, ignore) < 6 && active_count(symbol, bucket, ignore) < 2;
  }

  double effective_k(Symbol symbol) const {
    const auto it = k_estimate_.find(symbol);
    if (it == k_estimate_.end()) {
      return 25.0;
    }
    return std::clamp(it->second, 1.0, 99.0);
  }

  Qty exo_flow_hat(Symbol symbol) const {
    const auto it = exo_flow_estimate_.find(symbol);
    return it == exo_flow_estimate_.end() ? 0 : it->second;
  }

  int recent_event_signed_qty(Symbol symbol, TimestampUs now) const {
    const auto trades_it = recent_trades_.find(symbol);
    if (trades_it == recent_trades_.end()) {
      return 0;
    }
    int signed_qty = 0;
    for (const auto& trade : trades_it->second) {
      if (trade.source == "event_bot" && !trade.team_involved && now - trade.ts <= config_.event_lookback_us) {
        signed_qty += trade.aggressor_side == Side::Buy ? trade.qty : -trade.qty;
      }
    }
    return signed_qty;
  }

  int event_bias_ticks(Symbol symbol, TimestampUs now) const {
    const auto threshold = std::max<Qty>(1, config_.event_threshold_qty);
    return std::clamp(static_cast<int>(std::llround(static_cast<double>(recent_event_signed_qty(symbol, now)) /
                                                    static_cast<double>(threshold))),
                      -6, 6);
  }

  bool signal_live(Symbol symbol, TimestampUs now) const {
    return std::abs(recent_event_signed_qty(symbol, now)) >= config_.event_threshold_qty;
  }

  double settlement_hat(Symbol symbol) const {
    return 500.0 + static_cast<double>(exo_flow_hat(symbol)) / 100.0;
  }

  Qty target_delta(Symbol symbol) const {
    const auto raw = config_.competition_share * static_cast<double>(exo_flow_hat(symbol)) *
                     ((effective_k(symbol) / 100.0) - 1.0);
    return std::clamp(static_cast<Qty>(std::llround(raw)), -config_.max_abs_position, config_.max_abs_position);
  }

  std::map<Symbol, double> single_name_fairs(TimestampUs now) const {
    std::map<Symbol, double> fairs;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      fairs[symbol] = settlement_hat(symbol) + static_cast<double>(event_bias_ticks(symbol, now));
    }
    return fairs;
  }

  double challenge_etf_fair(const std::map<Symbol, double>& single_name_fairs) const {
    return (single_name_fairs.at(Symbol::E) + 2.0 * single_name_fairs.at(Symbol::T) +
            3.0 * single_name_fairs.at(Symbol::F)) /
           6.0;
  }

  Qty passive_quote_size(Symbol symbol) const {
    if (symbol == Symbol::ETF) {
      return config_.aggressive_qty;
    }
    return std::max<Qty>(1, config_.aggressive_qty / 2);
  }

  Qty aggressive_size(Symbol symbol) const {
    return passive_quote_size(symbol);
  }

  int quote_skew_ticks(Symbol symbol, const MarketSnapshot& snapshot, Qty target_delta_qty) const {
    const auto current = snapshot.positions.contains(symbol) ? snapshot.positions.at(symbol).qty : 0;
    const auto gap = target_delta_qty - current;
    return std::clamp(static_cast<int>(std::llround(static_cast<double>(gap) /
                                                    static_cast<double>(std::max<Qty>(1, config_.aggressive_qty)))),
                      -3, 3);
  }

  std::optional<PriceTick> quote_price(Symbol symbol, Side side, double fair, int skew_ticks) const {
    const auto adjusted_fair = fair + static_cast<double>(skew_ticks);
    const auto raw_price =
        side == Side::Buy ? adjusted_fair - config_.quote_width_ticks : adjusted_fair + config_.quote_width_ticks;
    return passive_quote_price(symbol, side, raw_price);
  }

  std::optional<OrderId>& quote_slot(Symbol symbol, Side side) {
    return side == Side::Buy ? quote_bid_ids_[symbol] : quote_ask_ids_[symbol];
  }

  void cancel_quote(Symbol symbol, Side side, StrategyDecision& decision) {
    auto& slot = quote_slot(symbol, side);
    if (slot && active_orders_.contains(*slot)) {
      decision.commands.push_back(Cancel{*slot, "stale_cancel"});
      stale_quote_marks_[*slot] = true;
      active_orders_.erase(*slot);
    }
    slot.reset();
  }

  void manage_quote_side(const MarketSnapshot& snapshot, Symbol symbol, Side side, double fair, Qty target_delta_qty,
                         StrategyDecision& decision) {
    const auto tag =
        "challenge_quote_" + to_string(symbol) + (side == Side::Buy ? "_bid" : "_ask");
    const auto qty = passive_quote_size(symbol);
    const auto delta = side == Side::Buy ? qty : -qty;
    const auto price = quote_price(symbol, side, fair, quote_skew_ticks(symbol, snapshot, target_delta_qty));
    auto& slot = quote_slot(symbol, side);

    if (!price || !within_risk(snapshot, symbol, delta)) {
      cancel_quote(symbol, side, decision);
      return;
    }

    if (slot && active_orders_.contains(*slot)) {
      auto& order = active_orders_.at(*slot);
      if (order.price == *price && order.remaining_qty == qty && order.tag == tag) {
        return;
      }
      order.price = *price;
      order.remaining_qty = qty;
      order.state = OrderState::PendingNew;
      order.tag = tag;
      decision.commands.push_back(CancelReplace{*slot, *price, qty, tag});
      return;
    }

    if (!has_capacity(symbol, OrderBucket::Quote)) {
      return;
    }

    const auto id = next_order_id();
    slot = id;
    active_orders_[id] = OrderView{id, symbol, side, *price, qty, OrderState::PendingNew, tag};
    decision.commands.push_back(PlaceLimit{id, symbol, side, *price, qty, tag});
  }

  StrategyDecision quote_symbol(const MarketSnapshot& snapshot, Symbol symbol, double fair, Qty target_delta_qty) {
    StrategyDecision decision;
    manage_quote_side(snapshot, symbol, Side::Buy, fair, target_delta_qty, decision);
    manage_quote_side(snapshot, symbol, Side::Sell, fair, target_delta_qty, decision);
    return decision;
  }

  StrategyDecision maybe_trade_basket(const MarketSnapshot& snapshot) {
    StrategyDecision decision;
    if (!best_bid(Symbol::ETF) || !best_ask(Symbol::ETF) || !best_bid(Symbol::E) || !best_bid(Symbol::T) ||
        !best_bid(Symbol::F) || !best_ask(Symbol::E) || !best_ask(Symbol::T) || !best_ask(Symbol::F)) {
      return decision;
    }

    const auto hedge = config_.hedge_unit;
    const auto rich_edge =
        6 * best_bid(Symbol::ETF).value() -
        (best_ask(Symbol::E).value() + 2 * best_ask(Symbol::T).value() + 3 * best_ask(Symbol::F).value());
    const auto cheap_edge =
        (best_bid(Symbol::E).value() + 2 * best_bid(Symbol::T).value() + 3 * best_bid(Symbol::F).value()) -
        6 * best_ask(Symbol::ETF).value();

    decision.diagnostics["rich_edge"] = rich_edge;
    decision.diagnostics["cheap_edge"] = cheap_edge;

    const auto can_place_rich =
        rich_edge >= config_.edge_buffer_ticks && !has_active_tag("basket_arb") &&
        within_risk(snapshot, Symbol::ETF, -6 * hedge) && within_risk(snapshot, Symbol::E, hedge) &&
        within_risk(snapshot, Symbol::T, 2 * hedge) && within_risk(snapshot, Symbol::F, 3 * hedge) &&
        has_capacity(Symbol::ETF, OrderBucket::Hedge) && has_capacity(Symbol::E, OrderBucket::Hedge) &&
        has_capacity(Symbol::T, OrderBucket::Hedge) && has_capacity(Symbol::F, OrderBucket::Hedge);
    const auto can_place_cheap =
        cheap_edge >= config_.edge_buffer_ticks && !has_active_tag("basket_arb") &&
        within_risk(snapshot, Symbol::ETF, 6 * hedge) && within_risk(snapshot, Symbol::E, -hedge) &&
        within_risk(snapshot, Symbol::T, -2 * hedge) && within_risk(snapshot, Symbol::F, -3 * hedge) &&
        has_capacity(Symbol::ETF, OrderBucket::Hedge) && has_capacity(Symbol::E, OrderBucket::Hedge) &&
        has_capacity(Symbol::T, OrderBucket::Hedge) && has_capacity(Symbol::F, OrderBucket::Hedge);

    if (can_place_rich) {
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
    } else if (can_place_cheap) {
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
    return decision;
  }

  EventSignal strongest_event_signal(TimestampUs now) const {
    EventSignal best;
    int best_score = 0;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      const auto signed_qty = recent_event_signed_qty(symbol, now);
      const auto score = std::abs(signed_qty) * symbol_weight(symbol);
      if (score > best_score && std::abs(signed_qty) >= config_.event_threshold_qty) {
        best_score = score;
        best = EventSignal{symbol, signed_qty, true};
      }
    }
    return best;
  }

  void maybe_place_event_order(const MarketSnapshot& snapshot, Symbol symbol, Side side, double fair,
                               StrategyDecision& decision) {
    if (!best_bid(symbol) || !best_ask(symbol)) {
      return;
    }
    const auto market_mid = mid_price(books_.at(symbol));
    if ((side == Side::Buy && fair <= market_mid) || (side == Side::Sell && fair >= market_mid)) {
      return;
    }
    if (has_active_order(symbol, side, "event_reprice") || !has_capacity(symbol, OrderBucket::Event)) {
      return;
    }

    const auto qty = aggressive_size(symbol);
    const auto delta = side == Side::Buy ? qty : -qty;
    if (!within_risk(snapshot, symbol, delta)) {
      return;
    }

    const auto tag =
        "challenge_event_reprice_" + std::string(side == Side::Buy ? "buy_" : "sell_") + to_string(symbol);
    decision.commands.push_back(PlaceLimit{
        next_order_id(), symbol, side, side == Side::Buy ? best_ask(symbol).value() : best_bid(symbol).value(),
        qty, tag});
  }

  void maybe_place_unwind(const MarketSnapshot& snapshot, Symbol symbol, StrategyDecision& decision) {
    const auto current = snapshot.positions.contains(symbol) ? snapshot.positions.at(symbol).qty : 0;
    if (std::abs(current) < aggressive_size(symbol) || !best_bid(symbol) || !best_ask(symbol)) {
      return;
    }
    const auto side = current > 0 ? Side::Sell : Side::Buy;
    if (has_active_order(symbol, side, "challenge_unwind") || !has_capacity(symbol, OrderBucket::Hedge)) {
      return;
    }

    const auto qty = std::min<Qty>(aggressive_size(symbol), std::abs(current));
    const auto delta = side == Side::Buy ? qty : -qty;
    if (!within_risk(snapshot, symbol, delta)) {
      return;
    }

    const auto tag =
        "challenge_unwind_" + std::string(side == Side::Buy ? "buy_" : "sell_") + to_string(symbol);
    decision.commands.push_back(PlaceLimit{
        next_order_id(), symbol, side, side == Side::Buy ? best_ask(symbol).value() : best_bid(symbol).value(),
        qty, tag});
  }

  StrategyDecision manage_event_and_unwind(const MarketSnapshot& snapshot, const std::map<Symbol, double>& fairs,
                                           double etf_fair) {
    StrategyDecision decision;
    const auto signal = strongest_event_signal(snapshot.now);
    decision.diagnostics["signal_symbol"] = signal.active ? to_string(signal.symbol) : "none";
    decision.diagnostics["signal_signed_qty"] = signal.signed_qty;

    if (signal.active) {
      const auto side = signal.signed_qty > 0 ? Side::Buy : Side::Sell;
      const auto stale_side = signal.signed_qty > 0 ? Side::Sell : Side::Buy;
      cancel_quote(signal.symbol, stale_side, decision);
      cancel_quote(Symbol::ETF, stale_side, decision);
      maybe_place_event_order(snapshot, signal.symbol, side, fairs.at(signal.symbol), decision);
      maybe_place_event_order(snapshot, Symbol::ETF, side, etf_fair, decision);
    } else {
      for (const auto symbol : all_symbols()) {
        maybe_place_unwind(snapshot, symbol, decision);
      }
    }
    return decision;
  }

  StrategyDecision evaluate(const MarketSnapshot& snapshot) {
    const auto fairs = single_name_fairs(snapshot.now);
    const auto etf_fair = challenge_etf_fair(fairs);

    StrategyDecision decision;
    decision.diagnostics["etf_fair"] = etf_fair;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      decision.diagnostics["single_fair_" + to_string(symbol)] = fairs.at(symbol);
      decision.diagnostics["settlement_hat_" + to_string(symbol)] = settlement_hat(symbol);
      decision.diagnostics["event_bias_" + to_string(symbol)] = event_bias_ticks(symbol, snapshot.now);
      decision.diagnostics["exo_flow_hat_" + to_string(symbol)] = exo_flow_hat(symbol);
      decision.diagnostics["target_delta_" + to_string(symbol)] = target_delta(symbol);
      decision.diagnostics["k_estimate_" + to_string(symbol)] = effective_k(symbol);
    }

    decision = merge(std::move(decision), maybe_trade_basket(snapshot));
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      decision = merge(std::move(decision), quote_symbol(snapshot, symbol, fairs.at(symbol), target_delta(symbol)));
    }
    decision = merge(std::move(decision), quote_symbol(snapshot, Symbol::ETF, etf_fair, 0));
    decision = merge(std::move(decision), manage_event_and_unwind(snapshot, fairs, etf_fair));
    return decision;
  }

  StrategyDecision react_to_market(const MarketSnapshot& snapshot) {
    const auto fairs = single_name_fairs(snapshot.now);
    const auto etf_fair = challenge_etf_fair(fairs);

    StrategyDecision decision;
    decision.diagnostics["etf_fair"] = etf_fair;
    for (const auto symbol : {Symbol::E, Symbol::T, Symbol::F}) {
      decision.diagnostics["single_fair_" + to_string(symbol)] = fairs.at(symbol);
      decision.diagnostics["settlement_hat_" + to_string(symbol)] = settlement_hat(symbol);
      decision.diagnostics["event_bias_" + to_string(symbol)] = event_bias_ticks(symbol, snapshot.now);
      decision.diagnostics["exo_flow_hat_" + to_string(symbol)] = exo_flow_hat(symbol);
      decision.diagnostics["target_delta_" + to_string(symbol)] = target_delta(symbol);
      decision.diagnostics["k_estimate_" + to_string(symbol)] = effective_k(symbol);
    }
    return merge(std::move(decision), manage_event_and_unwind(snapshot, fairs, etf_fair));
  }

  std::map<Symbol, std::optional<OrderId>> quote_bid_ids_;
  std::map<Symbol, std::optional<OrderId>> quote_ask_ids_;
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
  if (config.name == "challenge_v1") {
    return std::make_unique<ChallengeV1Strategy>(config);
  }
  throw std::invalid_argument("unknown strategy: " + config.name);
}

}  // namespace etf
