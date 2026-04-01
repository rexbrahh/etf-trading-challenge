#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <random>
#include <unordered_map>

#include "etf/core.hpp"

namespace etf {

class RunLogger;

class Simulator {
 public:
  explicit Simulator(AppConfig config, RunLogger* logger = nullptr);

  void connect();
  std::vector<MarketEvent> poll();
  void submit(const std::vector<OrderCommand>& commands);
  void flush();
  void disconnect();

  const MarketSnapshot& snapshot() const { return snapshot_; }
  SummaryStats summary() const { return summary_; }

 private:
  struct ScheduledAction;
  struct RestingOrder;

  void initialize_books();
  void schedule_initial_actions();
  void schedule_timer_tick(TimestampUs ts);
  void schedule_noise_tick(Symbol symbol, TimestampUs ts);
  void schedule_event_wave(TimestampUs ts);
  void schedule_passive_opponent_tick(Symbol symbol, TimestampUs ts);
  void schedule_naive_arb_tick(TimestampUs ts);
  void schedule_maker_refresh(Symbol symbol, TimestampUs ts);

  void process_noise(Symbol symbol);
  void process_event(Symbol symbol, Side side);
  void refresh_maker_quotes(Symbol symbol);
  void refresh_passive_opponent(Symbol symbol);
  void process_naive_arb();

  void queue_event(const MarketEvent& event);
  std::vector<MarketEvent> drain_pending();
  void emit_book_if_changed(Symbol symbol);
  void process_command(const OrderCommand& command);
  void place_limit(OrderId order_id, Symbol symbol, Side side, PriceTick price, Qty qty,
                   const std::string& tag, bool team_owned);
  void cancel_order(OrderId order_id);
  void cancel_replace(OrderId order_id, PriceTick new_price, Qty new_qty, const std::string& tag);

  void aggress_symbol(Symbol symbol, Side side, Qty qty, const std::string& source, bool team_owned,
                      std::optional<OrderId> order_id = std::nullopt, const std::string& tag = {});
  void insert_order(const RestingOrder& order);
  void remove_order(OrderId order_id);
  std::optional<OrderId> best_order_id(Symbol symbol, Side side) const;
  TopOfBook compute_top(Symbol symbol) const;

  Qty displayed_best_qty(Symbol symbol, Side side) const;
  Qty flow_size_for_width(Symbol symbol, Qty base_qty) const;
  double book_width(Symbol symbol) const;

  void update_team_position(const Fill& fill);
  void update_owner_position(const std::string& owner_key, Side passive_side, Qty qty);
  void recompute_pnl();
  void schedule(std::function<void()> fn, TimestampUs ts);

  AppConfig config_;
  RunLogger* logger_{nullptr};
  bool connected_{false};
  bool finished_{false};
  TimestampUs now_{0};
  std::uint64_t sequence_{0};

  struct MarketViewState {
    TopOfBook current;
    TopOfBook last_emitted;
  };

  std::map<Symbol, MarketViewState> books_;
  MarketSnapshot snapshot_;
  SummaryStats summary_;

  struct ScheduledAction {
    TimestampUs ts{0};
    std::uint64_t seq{0};
    std::function<void()> fn;

    bool operator<(const ScheduledAction& other) const {
      if (ts != other.ts) {
        return ts > other.ts;
      }
      return seq > other.seq;
    }
  };

  struct RestingOrder {
    OrderId id{0};
    std::string owner;
    Symbol symbol{Symbol::E};
    Side side{Side::Buy};
    PriceTick price{0};
    Qty qty{0};
    TimestampUs inserted_at{0};
    bool team_owned{false};
    bool opponent_owned{false};
    std::string tag;
  };

  std::priority_queue<ScheduledAction> schedule_;
  std::vector<MarketEvent> pending_events_;
  std::unordered_map<OrderId, RestingOrder> orders_;
  std::map<Symbol, std::map<PriceTick, std::vector<OrderId>, std::greater<PriceTick>>> bids_;
  std::map<Symbol, std::map<PriceTick, std::vector<OrderId>>> asks_;
  std::map<Symbol, double> maker_position_;
  std::map<Symbol, TimestampUs> maker_ready_ts_;
  std::map<Symbol, int> next_opponent_order_id_;
  std::map<Symbol, Qty> noise_positions_;
  std::map<Symbol, Qty> event_positions_;
  OrderId internal_order_id_{1'000'000};
  std::mt19937_64 rng_;
};

}  // namespace etf
