#include "etf/gateway.hpp"

#include <fstream>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "etf/exchange_protocol.hpp"
#include "etf/logging.hpp"
#include "etf/simulator.hpp"
#include "etf/strategy.hpp"

namespace etf {

namespace {

class SimGateway final : public MarketGateway {
 public:
  explicit SimGateway(AppConfig config, RunLogger* logger = nullptr)
      : simulator_(std::move(config), logger) {}

  void connect() override { simulator_.connect(); }
  std::vector<MarketEvent> poll() override { return simulator_.poll(); }
  void submit(const std::vector<OrderCommand>& commands) override { simulator_.submit(commands); }
  void flush() override { simulator_.flush(); }
  void disconnect() override { simulator_.disconnect(); }

  SummaryStats summary() const { return simulator_.summary(); }

 private:
  Simulator simulator_;
};

MarketEvent parse_market_event(const nlohmann::json& json) {
  const auto& event = json.at("event");
  const auto type = event.at("type").get<std::string>();
  if (type == "book_update") {
    const auto& book = event.at("book");
    TopOfBook top;
    top.symbol = symbol_from_string(book.at("symbol").get<std::string>());
    if (!book.at("bid").is_null()) {
      top.bid = BookLevel{book.at("bid").at("price").get<PriceTick>(), book.at("bid").at("qty").get<Qty>()};
    }
    if (!book.at("ask").is_null()) {
      top.ask = BookLevel{book.at("ask").at("price").get<PriceTick>(), book.at("ask").at("qty").get<Qty>()};
    }
    return BookUpdate{event.at("ts").get<TimestampUs>(), top};
  }
  if (type == "trade_print") {
    TradePrint trade;
    trade.ts = event.at("ts").get<TimestampUs>();
    trade.symbol = symbol_from_string(event.at("symbol").get<std::string>());
    trade.aggressor_side = side_from_string(event.at("aggressor_side").get<std::string>());
    trade.price = event.at("price").get<PriceTick>();
    trade.qty = event.at("qty").get<Qty>();
    trade.source = event.at("source").get<std::string>();
    if (!event.at("resting_order_id").is_null()) {
      trade.resting_order_id = event.at("resting_order_id").get<OrderId>();
    }
    trade.team_involved = event.at("team_involved").get<bool>();
    return trade;
  }
  if (type == "ack") {
    return Ack{event.at("ts").get<TimestampUs>(), event.at("order_id").get<OrderId>(),
               symbol_from_string(event.at("symbol").get<std::string>()),
               side_from_string(event.at("side").get<std::string>()), event.at("price").get<PriceTick>(),
               event.at("qty").get<Qty>(), event.at("tag").get<std::string>()};
  }
  if (type == "fill") {
    return Fill{event.at("ts").get<TimestampUs>(),
                symbol_from_string(event.at("symbol").get<std::string>()),
                event.at("order_id").get<OrderId>(),
                side_from_string(event.at("side").get<std::string>()), event.at("price").get<PriceTick>(),
                event.at("qty").get<Qty>(), event.at("aggressor").get<bool>(),
                event.at("source").get<std::string>(), event.at("tag").get<std::string>()};
  }
  if (type == "cancel_ack") {
    return CancelAck{event.at("ts").get<TimestampUs>(), event.at("order_id").get<OrderId>(),
                     symbol_from_string(event.at("symbol").get<std::string>())};
  }
  if (type == "reject") {
    Reject reject;
    reject.ts = event.at("ts").get<TimestampUs>();
    reject.symbol = symbol_from_string(event.at("symbol").get<std::string>());
    reject.reason = event.at("reason").get<std::string>();
    if (!event.at("order_id").is_null()) {
      reject.order_id = event.at("order_id").get<OrderId>();
    }
    return reject;
  }
  return Timer{event.at("ts").get<TimestampUs>(), event.at("name").get<std::string>()};
}

class ReplayGateway final : public MarketGateway {
 public:
  ReplayGateway(AppConfig config, RunLogger* logger = nullptr) : logger_(logger) {
    if (config.run.replay_input_path.empty()) {
      throw std::invalid_argument("replay_input_path is required for replay");
    }
    std::ifstream in(config.run.replay_input_path);
    if (!in.is_open()) {
      throw std::runtime_error("unable to open replay log");
    }
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      const auto json = nlohmann::json::parse(line);
      if (json.at("kind").get<std::string>() == "market_event") {
        events_.push_back(parse_market_event(json));
      }
    }
  }

  void connect() override { connected_ = true; }

  std::vector<MarketEvent> poll() override {
    if (!connected_ || index_ >= events_.size()) {
      return {};
    }
    std::vector<MarketEvent> batch;
    const auto ts = event_timestamp(events_[index_]);
    last_ts_ = ts;
    while (index_ < events_.size() && event_timestamp(events_[index_]) == ts) {
      if (logger_) {
        logger_->log_market_event(events_[index_]);
      }
      batch.push_back(events_[index_++]);
    }
    return batch;
  }

  void submit(const std::vector<OrderCommand>& commands) override {
    if (!logger_) {
      return;
    }
    for (const auto& command : commands) {
      logger_->log_order_command(last_ts_, command, {{"replay", true}});
    }
  }

  void flush() override {}
  void disconnect() override { connected_ = false; }

 private:
  RunLogger* logger_{nullptr};
  bool connected_{false};
  std::vector<MarketEvent> events_;
  std::size_t index_{0};
  TimestampUs last_ts_{0};
};

class WebGatewayStub final : public MarketGateway {
 public:
  explicit WebGatewayStub(AppConfig config) : config_(std::move(config)) {}

  void connect() override {
    throw std::runtime_error("web gateway not implemented yet. The live Exchange interface uses "
                             "POST /auth/user/login with an access token, then a session-cookie-backed "
                             "websocket at " +
                             exchange_websocket_url(config_.run.live) +
                             ". See docs/exchange_live_protocol.md and `etf_lab live-protocol`.");
  }
  std::vector<MarketEvent> poll() override { return {}; }
  void submit(const std::vector<OrderCommand>&) override {}
  void flush() override {}
  void disconnect() override {}

 private:
  AppConfig config_;
};

void apply_event_to_snapshot(MarketSnapshot& snapshot, const MarketEvent& event) {
  snapshot.now = event_timestamp(event);
  std::visit(
      [&snapshot](const auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, BookUpdate>) {
          snapshot.books[payload.book.symbol] = payload.book;
        } else if constexpr (std::is_same_v<T, Ack>) {
          snapshot.active_orders[payload.order_id] =
              OrderView{payload.order_id, payload.symbol, payload.side, payload.price, payload.qty, OrderState::Active,
                        payload.tag};
        } else if constexpr (std::is_same_v<T, Fill>) {
          apply_fill(snapshot.positions[payload.symbol], payload.side, payload.price, payload.qty);
          auto it = snapshot.active_orders.find(payload.order_id);
          if (it != snapshot.active_orders.end()) {
            it->second.remaining_qty -= payload.qty;
            it->second.state =
                it->second.remaining_qty > 0 ? OrderState::PartiallyFilled : OrderState::Filled;
            if (it->second.remaining_qty <= 0) {
              snapshot.active_orders.erase(it);
            }
          }
        } else if constexpr (std::is_same_v<T, CancelAck>) {
          snapshot.active_orders.erase(payload.order_id);
        } else if constexpr (std::is_same_v<T, Reject>) {
          if (payload.order_id) {
            snapshot.active_orders.erase(*payload.order_id);
          }
        }
      },
      event);

  snapshot.pnl = {};
  for (const auto symbol : all_symbols()) {
    snapshot.pnl.realized += snapshot.positions[symbol].realized;
    const auto book_it = snapshot.books.find(symbol);
    const auto mark = book_it != snapshot.books.end() ? mid_price(book_it->second) : 500.0;
    snapshot.pnl.unrealized += unrealized_for_position(snapshot.positions[symbol], mark);
  }
  snapshot.pnl.total = snapshot.pnl.realized + snapshot.pnl.unrealized;
}

}  // namespace

std::unique_ptr<MarketGateway> make_sim_gateway(const AppConfig& config) {
  return std::make_unique<SimGateway>(config);
}

std::unique_ptr<MarketGateway> make_sim_gateway(const AppConfig& config, RunLogger* logger) {
  return std::make_unique<SimGateway>(config, logger);
}

std::unique_ptr<MarketGateway> make_replay_gateway(const AppConfig& config) {
  return std::make_unique<ReplayGateway>(config);
}

std::unique_ptr<MarketGateway> make_replay_gateway(const AppConfig& config, RunLogger* logger) {
  return std::make_unique<ReplayGateway>(config, logger);
}

std::unique_ptr<MarketGateway> make_web_gateway_stub(const AppConfig& config) {
  return std::make_unique<WebGatewayStub>(config);
}

MarketSnapshot run_gateway_session(MarketGateway& gateway, Strategy& strategy, AppConfig config,
                                   SummaryStats* summary, RunLogger* logger) {
  MarketSnapshot snapshot;
  snapshot.now = 0;
  snapshot.books = config.simulation.initial_books;
  for (const auto symbol : all_symbols()) {
    snapshot.positions[symbol] = {};
  }

  gateway.connect();
  auto start_decision = strategy.on_start(snapshot);
  if (logger) {
    logger->log_decision(snapshot.now, strategy.name(), start_decision.diagnostics);
  }
  gateway.submit(start_decision.commands);

  int arb_orders = 0;
  int event_orders = 0;
  std::unordered_map<OrderId, bool> stale_marked;

  for (const auto& command : start_decision.commands) {
    const auto json = command_to_json(command);
    if (json.at("type").get<std::string>() == "place_limit") {
      const auto tag = json.at("tag").get<std::string>();
      if (tag.find("basket_arb") != std::string::npos) {
        ++arb_orders;
      }
      if (tag.find("event_reprice") != std::string::npos) {
        ++event_orders;
      }
    } else if (json.at("type").get<std::string>() == "cancel" &&
               json.at("reason").get<std::string>() == "stale_cancel") {
      stale_marked[json.at("order_id").get<OrderId>()] = true;
    }
  }

  while (true) {
    auto events = gateway.poll();
    if (events.empty()) {
      break;
    }
    for (const auto& event : events) {
      apply_event_to_snapshot(snapshot, event);
      auto decision = std::holds_alternative<Timer>(event)
                          ? strategy.on_timer(std::get<Timer>(event), snapshot)
                          : strategy.on_event(event, snapshot);
      if (logger) {
        logger->log_decision(snapshot.now, strategy.name(), decision.diagnostics);
      }
      gateway.submit(decision.commands);
      for (const auto& command : decision.commands) {
        const auto json = command_to_json(command);
        if (json.at("type").get<std::string>() == "place_limit") {
          const auto tag = json.at("tag").get<std::string>();
          if (tag.find("basket_arb") != std::string::npos) {
            ++arb_orders;
          }
          if (tag.find("event_reprice") != std::string::npos) {
            ++event_orders;
          }
        } else if (json.at("type").get<std::string>() == "cancel" &&
                   json.at("reason").get<std::string>() == "stale_cancel") {
          stale_marked[json.at("order_id").get<OrderId>()] = true;
        }
      }

      if (std::holds_alternative<Fill>(event)) {
        const auto& fill = std::get<Fill>(event);
        if (logger) {
          logger->log_snapshot(snapshot);
        }
        if (summary) {
          if (fill.tag.find("basket_arb") != std::string::npos) {
            summary->arb_fills += 1;
            summary->fill_breakdown["basket_arb"] += fill.qty;
          } else if (fill.tag.find("event_reprice") != std::string::npos) {
            summary->fill_breakdown["event_reprice"] += fill.qty;
          } else if (fill.tag.find("quote") != std::string::npos) {
            summary->fill_breakdown["quotes"] += fill.qty;
          } else {
            summary->fill_breakdown["other"] += fill.qty;
          }
        }
        if (summary && stale_marked.contains(fill.order_id)) {
          summary->stale_quote_fills += 1;
        }
      }
    }
  }

  auto end_decision = strategy.on_end(snapshot);
  if (logger) {
    logger->log_decision(snapshot.now, strategy.name(), end_decision.diagnostics);
    logger->log_snapshot(snapshot);
  }
  gateway.submit(end_decision.commands);
  gateway.flush();
  gateway.disconnect();

  if (summary) {
    summary->strategy_name = strategy.name();
    summary->seed = config.simulation.seed;
    summary->pnl = snapshot.pnl;
    summary->arb_orders = arb_orders;
    summary->event_orders = event_orders;
    summary->final_positions.clear();
    for (const auto& [symbol, position] : snapshot.positions) {
      summary->final_positions[symbol] = position.qty;
    }
  }
  return snapshot;
}

}  // namespace etf
