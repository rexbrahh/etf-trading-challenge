#include <catch2/catch_test_macros.hpp>

#include <algorithm>

#include "etf/simulator.hpp"

namespace {

etf::AppConfig base_config() {
  auto config = etf::load_config_from_path("/Users/rexliu/etf-trading-challenge/configs/default.json");
  config.simulation.duration_us = 60'000;
  config.simulation.noise_interval_us = 80'000;
  config.simulation.event_interval_us = 15'000;
  config.simulation.strategy_timer_us = 80'000;
  config.simulation.maker_reload_us = 5'000;
  config.run.log_output_path.clear();
  return config;
}

}  // namespace

TEST_CASE("event bots never target ETF") {
  auto config = base_config();
  etf::Simulator simulator(config);
  simulator.connect();

  bool saw_event_trade = false;
  for (;;) {
    const auto events = simulator.poll();
    if (events.empty()) {
      break;
    }
    for (const auto& event : events) {
      if (std::holds_alternative<etf::TradePrint>(event)) {
        const auto& trade = std::get<etf::TradePrint>(event);
        if (trade.source == "event_bot") {
          saw_event_trade = true;
          REQUIRE(trade.symbol != etf::Symbol::ETF);
        }
      }
    }
  }
  REQUIRE(saw_event_trade);
}

TEST_CASE("maker reload delay delays reposting after fills") {
  auto config = base_config();
  config.simulation.event_interval_us = 200'000;
  etf::Simulator simulator(config);
  simulator.connect();

  auto initial = simulator.poll();
  REQUIRE_FALSE(initial.empty());

  simulator.submit({etf::PlaceLimit{1, etf::Symbol::E, etf::Side::Buy, config.simulation.price_max, 20, "test_hit"}});

  auto immediate = simulator.poll();
  REQUIRE_FALSE(immediate.empty());
  REQUIRE(std::any_of(immediate.begin(), immediate.end(),
                      [](const auto& event) { return std::holds_alternative<etf::Fill>(event); }));

  const auto delayed = simulator.poll();
  REQUIRE_FALSE(delayed.empty());
  REQUIRE(std::all_of(delayed.begin(), delayed.end(), [&](const auto& event) {
    return etf::event_timestamp(event) >= config.simulation.maker_reload_us;
  }));
}
