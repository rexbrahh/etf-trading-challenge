#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include <filesystem>
#include <fstream>
#include <vector>

#include "etf/gateway.hpp"
#include "etf/logging.hpp"
#include "etf/strategy.hpp"

namespace {

etf::MarketSnapshot make_snapshot() {
  etf::MarketSnapshot snapshot;
  snapshot.books[etf::Symbol::E] = etf::TopOfBook{etf::Symbol::E, etf::BookLevel{509, 10}, etf::BookLevel{511, 10}};
  snapshot.books[etf::Symbol::T] = etf::TopOfBook{etf::Symbol::T, etf::BookLevel{499, 10}, etf::BookLevel{501, 10}};
  snapshot.books[etf::Symbol::F] = etf::TopOfBook{etf::Symbol::F, etf::BookLevel{499, 10}, etf::BookLevel{501, 10}};
  snapshot.books[etf::Symbol::ETF] =
      etf::TopOfBook{etf::Symbol::ETF, etf::BookLevel{499, 10}, etf::BookLevel{501, 10}};
  for (const auto symbol : etf::all_symbols()) {
    snapshot.positions[symbol] = {};
  }
  snapshot.now = 10'000;
  return snapshot;
}

std::vector<nlohmann::json> load_lines(const std::string& path, const std::string& kind) {
  std::ifstream in(path);
  std::vector<nlohmann::json> out;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    auto json = nlohmann::json::parse(line);
    if (json.at("kind").get<std::string>() == kind) {
      out.push_back(json);
    }
  }
  return out;
}

}  // namespace

TEST_CASE("basket arb only fires when executable edge clears threshold") {
  etf::StrategyConfig config;
  config.name = "basket_arb";
  config.edge_buffer_ticks = 2;
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  const auto no_trade = strategy->on_timer(etf::Timer{snapshot.now, "timer"}, snapshot);
  REQUIRE(no_trade.commands.empty());

  snapshot.books[etf::Symbol::ETF] =
      etf::TopOfBook{etf::Symbol::ETF, etf::BookLevel{504, 10}, etf::BookLevel{506, 10}};
  const auto yes_trade = strategy->on_timer(etf::Timer{snapshot.now, "timer"}, snapshot);
  REQUIRE_FALSE(yes_trade.commands.empty());
}

TEST_CASE("event propagation uses weighted ETF fair and cancels stale ask before buying") {
  etf::StrategyConfig config;
  config.name = "event_prop";
  config.event_threshold_qty = 8;
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  const auto start = strategy->on_start(snapshot);
  REQUIRE(start.commands.size() == 2);

  for (const auto& command : start.commands) {
    const auto json = etf::command_to_json(command);
    const auto order_id = json.at("order_id").get<etf::OrderId>();
    const auto symbol = etf::symbol_from_string(json.at("symbol").get<std::string>());
    const auto side = etf::side_from_string(json.at("side").get<std::string>());
    strategy->on_event(
        etf::Ack{snapshot.now, order_id, symbol, side, json.at("price").get<etf::PriceTick>(),
                 json.at("qty").get<etf::Qty>(), json.at("tag").get<std::string>()},
        snapshot);
  }

  snapshot.now += 100;
  const auto decision = strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::E, etf::Side::Buy, 511, 10, "event_bot", std::nullopt, false},
      snapshot);

  REQUIRE(decision.diagnostics.at("implied_etf").get<double>() ==
          Catch::Approx((510.0 + 2 * 500.0 + 3 * 500.0) / 6.0));
  REQUIRE(decision.commands.size() >= 2);
  REQUIRE(std::holds_alternative<etf::Cancel>(decision.commands.front()));
  REQUIRE(std::holds_alternative<etf::PlaceLimit>(decision.commands.back()));
}

TEST_CASE("risk caps suppress new hybrid exposure") {
  etf::StrategyConfig config;
  config.name = "hybrid";
  config.max_abs_position = 5;
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  snapshot.positions[etf::Symbol::ETF].qty = 5;
  const auto decision = strategy->on_timer(etf::Timer{snapshot.now, "timer"}, snapshot);
  for (const auto& command : decision.commands) {
    if (std::holds_alternative<etf::PlaceLimit>(command)) {
      const auto order = std::get<etf::PlaceLimit>(command);
      REQUIRE_FALSE((order.side == etf::Side::Buy && order.symbol == etf::Symbol::ETF));
    }
  }
}

TEST_CASE("challenge_v1 target delta turns negative on positive exogenous flow with low k") {
  etf::StrategyConfig config;
  config.name = "challenge_v1";
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  snapshot.books[etf::Symbol::E] = etf::TopOfBook{etf::Symbol::E, etf::BookLevel{519, 10}, etf::BookLevel{521, 10}};
  const auto decision = strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::E, etf::Side::Buy, 521, 20, "noise_bot", std::nullopt, false},
      snapshot);

  REQUIRE(decision.diagnostics.at("target_delta_E").get<int>() < 0);
}

TEST_CASE("challenge_v1 weights F events triple E for ETF fair impact") {
  etf::StrategyConfig config;
  config.name = "challenge_v1";

  auto snapshot = make_snapshot();

  auto e_strategy = etf::make_strategy(config);
  const auto e_decision = e_strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::E, etf::Side::Buy, 511, config.event_threshold_qty, "event_bot",
                      std::nullopt, false},
      snapshot);
  const auto e_shift = e_decision.diagnostics.at("etf_fair").get<double>() - 500.0;

  auto f_strategy = etf::make_strategy(config);
  const auto f_decision = f_strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::F, etf::Side::Buy, 501, config.event_threshold_qty, "event_bot",
                      std::nullopt, false},
      snapshot);
  const auto f_shift = f_decision.diagnostics.at("etf_fair").get<double>() - 500.0;

  REQUIRE(f_shift == Catch::Approx(3.0 * e_shift));
}

TEST_CASE("challenge_v1 keeps passive quotes inside band and off the touch after clipping") {
  etf::StrategyConfig config;
  config.name = "challenge_v1";
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  for (const auto symbol : etf::all_symbols()) {
    snapshot.books[symbol] = etf::TopOfBook{symbol, etf::BookLevel{698, 10}, etf::BookLevel{700, 10}};
  }

  strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::E, etf::Side::Buy, 700, 25'000, "noise_bot", std::nullopt, false},
      snapshot);
  strategy->on_event(
      etf::TradePrint{snapshot.now + 1, etf::Symbol::T, etf::Side::Buy, 700, 25'000, "noise_bot", std::nullopt, false},
      snapshot);
  snapshot.now += 2;
  const auto decision = strategy->on_event(
      etf::TradePrint{snapshot.now, etf::Symbol::F, etf::Side::Buy, 700, 25'000, "noise_bot", std::nullopt, false},
      snapshot);

  for (const auto& command : decision.commands) {
    if (!std::holds_alternative<etf::PlaceLimit>(command)) {
      continue;
    }
    const auto order = std::get<etf::PlaceLimit>(command);
    REQUIRE(order.price >= etf::kMinPrice);
    REQUIRE(order.price <= etf::kMaxPrice);
    if (order.tag.find("quote") != std::string::npos) {
      const auto ask = snapshot.books.at(order.symbol).ask->price;
      const auto bid = snapshot.books.at(order.symbol).bid->price;
      if (order.side == etf::Side::Buy) {
        REQUIRE(order.price < ask);
      } else {
        REQUIRE(order.price > bid);
      }
    }
  }
}

TEST_CASE("challenge_v1 respects per-symbol active order budget") {
  etf::StrategyConfig config;
  config.name = "challenge_v1";
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  strategy->on_event(etf::Ack{snapshot.now, 1, etf::Symbol::E, etf::Side::Buy, 498, 3, "challenge_quote_E_bid"}, snapshot);
  strategy->on_event(etf::Ack{snapshot.now, 2, etf::Symbol::E, etf::Side::Sell, 502, 3, "challenge_quote_E_ask"}, snapshot);
  strategy->on_event(etf::Ack{snapshot.now, 3, etf::Symbol::E, etf::Side::Buy, 497, 3, "challenge_unwind_buy_E"}, snapshot);
  strategy->on_event(etf::Ack{snapshot.now, 4, etf::Symbol::E, etf::Side::Sell, 503, 3, "challenge_unwind_sell_E"}, snapshot);
  strategy->on_event(etf::Ack{snapshot.now, 5, etf::Symbol::E, etf::Side::Buy, 501, 3, "challenge_event_reprice_buy_E"}, snapshot);
  strategy->on_event(etf::Ack{snapshot.now, 6, etf::Symbol::E, etf::Side::Sell, 499, 3, "challenge_event_reprice_sell_E"}, snapshot);

  const auto decision = strategy->on_timer(etf::Timer{snapshot.now, "timer"}, snapshot);
  for (const auto& command : decision.commands) {
    if (std::holds_alternative<etf::PlaceLimit>(command)) {
      REQUIRE(std::get<etf::PlaceLimit>(command).symbol != etf::Symbol::E);
    }
  }
}

TEST_CASE("challenge_v1 basket arb is all or none across risk checks") {
  etf::StrategyConfig config;
  config.name = "challenge_v1";
  auto strategy = etf::make_strategy(config);

  auto snapshot = make_snapshot();
  snapshot.books[etf::Symbol::ETF] =
      etf::TopOfBook{etf::Symbol::ETF, etf::BookLevel{504, 10}, etf::BookLevel{506, 10}};
  snapshot.positions[etf::Symbol::F].qty = config.max_abs_position;

  const auto decision = strategy->on_timer(etf::Timer{snapshot.now, "timer"}, snapshot);
  for (const auto& command : decision.commands) {
    if (std::holds_alternative<etf::PlaceLimit>(command)) {
      REQUIRE(std::get<etf::PlaceLimit>(command).tag.find("basket_arb") == std::string::npos);
    }
  }
}

TEST_CASE("simulator is deterministic and replay reproduces order decisions") {
  auto config = etf::load_config_from_path("/Users/rexliu/etf-trading-challenge/configs/default.json");
  config.simulation.duration_us = 40'000;
  config.run.log_output_path = "/Users/rexliu/etf-trading-challenge/tmp/test_det_a.ndjson";
  const auto first = etf::simulate_to_log(config);

  auto second_config = config;
  second_config.run.log_output_path = "/Users/rexliu/etf-trading-challenge/tmp/test_det_b.ndjson";
  const auto second = etf::simulate_to_log(second_config);
  REQUIRE(first.pnl.total == Catch::Approx(second.pnl.total));

  auto replay_config = config;
  replay_config.run.replay_input_path = config.run.log_output_path;
  replay_config.run.log_output_path = "/Users/rexliu/etf-trading-challenge/tmp/test_replay.ndjson";
  etf::replay_to_log(replay_config);

  const auto original_orders = load_lines(config.run.log_output_path, "order_command");
  const auto replay_orders = load_lines(replay_config.run.log_output_path, "order_command");
  REQUIRE(original_orders.size() == replay_orders.size());
  for (std::size_t i = 0; i < original_orders.size(); ++i) {
    REQUIRE(original_orders[i].at("command") == replay_orders[i].at("command"));
  }

  std::filesystem::remove(config.run.log_output_path);
  std::filesystem::remove(second_config.run.log_output_path);
  std::filesystem::remove(replay_config.run.log_output_path);
}
