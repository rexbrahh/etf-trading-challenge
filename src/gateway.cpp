#include "etf/gateway.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "etf/exchange_protocol.hpp"
#include "etf/logging.hpp"
#include "etf/simulator.hpp"
#include "etf/strategy.hpp"

namespace etf {

namespace {

using Clock = std::chrono::steady_clock;

TimestampUs wall_now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch()).count();
}

std::string errno_message(const std::string& context) {
  return context + ": " + std::strerror(errno);
}

std::string resolve_existing_path(const std::vector<std::string>& candidates) {
  for (const auto& candidate : candidates) {
    if (std::filesystem::exists(candidate)) {
      return candidate;
    }
  }
  throw std::runtime_error("unable to locate live bridge script");
}

int reserve_local_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error(errno_message("socket"));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const auto error = errno_message("bind");
    ::close(fd);
    throw std::runtime_error(error);
  }

  socklen_t size = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &size) < 0) {
    const auto error = errno_message("getsockname");
    ::close(fd);
    throw std::runtime_error(error);
  }
  const int port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

int connect_local_socket(int port, std::chrono::milliseconds timeout) {
  const auto deadline = Clock::now() + timeout;
  while (Clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      throw std::runtime_error(errno_message("socket"));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }

    ::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error("timed out connecting to live bridge on localhost:" + std::to_string(port));
}

void send_json_line(int fd, const nlohmann::json& json) {
  const auto line = json.dump() + "\n";
  std::size_t offset = 0;
  while (offset < line.size()) {
    const auto wrote = ::send(fd, line.data() + offset, line.size() - offset, 0);
    if (wrote < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(errno_message("send"));
    }
    offset += static_cast<std::size_t>(wrote);
  }
}

bool socket_readable(int fd, std::chrono::milliseconds timeout) {
  fd_set read_fds;
  FD_ZERO(&read_fds);
  FD_SET(fd, &read_fds);
  timeval tv{};
  tv.tv_sec = static_cast<int>(timeout.count() / 1000);
  tv.tv_usec = static_cast<int>((timeout.count() % 1000) * 1000);
  while (true) {
    const auto rc = ::select(fd + 1, &read_fds, nullptr, nullptr, &tv);
    if (rc < 0 && errno == EINTR) {
      continue;
    }
    if (rc < 0) {
      throw std::runtime_error(errno_message("select"));
    }
    return rc > 0;
  }
}

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

class SimGateway final : public MarketGateway {
 public:
  explicit SimGateway(AppConfig config, RunLogger* logger = nullptr)
      : simulator_(std::move(config), logger) {}

  void connect() override { simulator_.connect(); }
  std::vector<MarketEvent> poll() override { return simulator_.poll(); }
  void submit(const std::vector<OrderCommand>& commands) override { simulator_.submit(commands); }
  void flush() override { simulator_.flush(); }
  void disconnect() override { simulator_.disconnect(); }

 private:
  Simulator simulator_;
};

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

class LiveGateway final : public MarketGateway {
 public:
  explicit LiveGateway(AppConfig config, RunLogger* logger = nullptr)
      : config_(std::move(config)), logger_(logger) {}

  ~LiveGateway() override { disconnect(); }

  void connect() override {
    if (!config_.run.live.enabled) {
      throw std::invalid_argument("run.live.enabled must be true for live gateway");
    }
    if (config_.run.live.access_token.empty() || config_.run.live.access_token == "replace-me") {
      throw std::invalid_argument("run.live.access_token must be set before starting live mode");
    }

    const auto script = resolve_existing_path({"tools/live_bridge.mjs", "../tools/live_bridge.mjs"});
    port_ = reserve_local_port();
    const auto port_string = std::to_string(port_);

    child_pid_ = ::fork();
    if (child_pid_ < 0) {
      throw std::runtime_error(errno_message("fork"));
    }
    if (child_pid_ == 0) {
      ::execlp("node", "node", script.c_str(), "--port", port_string.c_str(), static_cast<char*>(nullptr));
      std::perror("execlp(node)");
      _exit(127);
    }

    socket_fd_ = connect_local_socket(port_, std::chrono::milliseconds(5'000));
    connected_ = true;
    send_json_line(socket_fd_, {{"type", "init"},
                                {"config", config_to_json(config_)},
                                {"protocol", "etf-live-bridge-v1"}});
  }

  std::vector<MarketEvent> poll() override {
    if (!connected_) {
      return {};
    }
    if (!queued_events_.empty()) {
      return drain_events();
    }
    if (finished_) {
      return {};
    }

    const auto now = wall_now_us();
    std::chrono::milliseconds timeout{250};
    if (trading_active_) {
      if (next_timer_due_us_ == 0) {
        next_timer_due_us_ = now + config_.simulation.strategy_timer_us;
      }
      const auto wait_us = std::max<TimestampUs>(0, next_timer_due_us_ - now);
      timeout = std::chrono::milliseconds(std::clamp<TimestampUs>(wait_us / 1000, 0, 1'000));
    }

    if (socket_readable(socket_fd_, timeout)) {
      read_bridge_messages();
    }

    if (!queued_events_.empty()) {
      return drain_events();
    }
    if (finished_) {
      return {};
    }

    if (trading_active_) {
      const auto after = wall_now_us();
      if (after >= next_timer_due_us_) {
        next_timer_due_us_ = after + config_.simulation.strategy_timer_us;
        return {Timer{after, "live_timer"}};
      }
    }
    return {};
  }

  void submit(const std::vector<OrderCommand>& commands) override {
    if (!connected_ || finished_ || !trading_active_) {
      return;
    }
    for (const auto& command : commands) {
      send_json_line(socket_fd_, {{"type", "command"}, {"command", command_to_json(command)}});
    }
  }

  void flush() override {}

  void disconnect() override {
    if (!connected_ && child_pid_ <= 0) {
      return;
    }

    if (connected_ && socket_fd_ >= 0) {
      try {
        send_json_line(socket_fd_, {{"type", "shutdown"}});
      } catch (...) {
      }
      ::close(socket_fd_);
      socket_fd_ = -1;
      connected_ = false;
    }

    if (child_pid_ > 0) {
      int status = 0;
      const auto waited = ::waitpid(child_pid_, &status, WNOHANG);
      if (waited == 0) {
        ::kill(child_pid_, SIGTERM);
        ::waitpid(child_pid_, &status, 0);
      }
      child_pid_ = -1;
    }
  }

  bool ready_for_strategy_start() const override {
    return trading_active_ &&
           live_books_seen_.size() >= std::max<std::size_t>(1, config_.run.live.subscribe_markets.size());
  }

  bool finished() const override { return finished_; }

 private:
  std::vector<MarketEvent> drain_events() {
    std::vector<MarketEvent> events;
    while (!queued_events_.empty()) {
      events.push_back(queued_events_.front());
      queued_events_.pop_front();
    }
    return events;
  }

  void read_bridge_messages() {
    char buffer[16 * 1024];
    while (true) {
      const auto received = ::recv(socket_fd_, buffer, sizeof(buffer), MSG_DONTWAIT);
      if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;
        }
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error(errno_message("recv"));
      }
      if (received == 0) {
        finished_ = true;
        break;
      }

      recv_buffer_.append(buffer, static_cast<std::size_t>(received));
      std::size_t newline = 0;
      while ((newline = recv_buffer_.find('\n')) != std::string::npos) {
        auto line = recv_buffer_.substr(0, newline);
        recv_buffer_.erase(0, newline + 1);
        if (!line.empty()) {
          handle_bridge_message(nlohmann::json::parse(line));
        }
      }
    }
  }

  void handle_bridge_message(const nlohmann::json& message) {
    const auto type = message.at("type").get<std::string>();
    if (type == "status") {
      trading_active_ = message.value("trading_active", false);
      if (!trading_active_) {
        next_timer_due_us_ = 0;
      }
      return;
    }
    if (type == "market_event") {
      auto event = parse_market_event(message);
      if (std::holds_alternative<BookUpdate>(event)) {
        live_books_seen_.insert(std::get<BookUpdate>(event).book.symbol);
      }
      if (logger_) {
        logger_->log_market_event(event);
      }
      queued_events_.push_back(std::move(event));
      return;
    }
    if (type == "end") {
      finished_ = true;
      trading_active_ = false;
      next_timer_due_us_ = 0;
      return;
    }
    if (type == "error") {
      finished_ = true;
      throw std::runtime_error("live bridge error: " + message.at("message").get<std::string>());
    }
  }

  AppConfig config_;
  RunLogger* logger_{nullptr};
  pid_t child_pid_{-1};
  int port_{0};
  int socket_fd_{-1};
  bool connected_{false};
  bool finished_{false};
  bool trading_active_{false};
  TimestampUs next_timer_due_us_{0};
  std::set<Symbol> live_books_seen_;
  std::deque<MarketEvent> queued_events_;
  std::string recv_buffer_;
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

std::unique_ptr<MarketGateway> make_live_gateway(const AppConfig& config) {
  return std::make_unique<LiveGateway>(config);
}

std::unique_ptr<MarketGateway> make_live_gateway(const AppConfig& config, RunLogger* logger) {
  return std::make_unique<LiveGateway>(config, logger);
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

  while (!gateway.ready_for_strategy_start()) {
    auto events = gateway.poll();
    if (events.empty()) {
      if (gateway.finished()) {
        gateway.disconnect();
        return snapshot;
      }
      continue;
    }
    for (const auto& event : events) {
      apply_event_to_snapshot(snapshot, event);
    }
  }

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
      if (gateway.finished()) {
        break;
      }
      if (config.run.live.enabled) {
        continue;
      }
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
