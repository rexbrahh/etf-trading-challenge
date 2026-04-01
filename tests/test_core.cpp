#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include <filesystem>
#include <fstream>

#include "etf/core.hpp"

TEST_CASE("settlement math matches packet formulas") {
  REQUIRE(etf::settlement_single(200, -100) == Catch::Approx(501.0));
  REQUIRE(etf::settlement_etf(510.0, 520.0, 530.0) == Catch::Approx((510.0 + 2 * 520.0 + 3 * 530.0) / 6.0));
}

TEST_CASE("runtime guards enforce packet limits") {
  REQUIRE_NOTHROW(etf::validate_price_tick(500));
  REQUIRE_THROWS(etf::validate_price_tick(299));
  REQUIRE_THROWS(etf::validate_price_tick(701));

  REQUIRE_NOTHROW(etf::validate_active_order_limit(9));
  REQUIRE_THROWS(etf::validate_active_order_limit(10));
}

TEST_CASE("helper models are monotonic in the intended direction") {
  REQUIRE(etf::maker_bot_fair(100.0, 25.0) == Catch::Approx(496.0));
  REQUIRE(etf::width_scaled_flow(2.0, 3) > etf::width_scaled_flow(6.0, 3));
}

TEST_CASE("side parsing accepts exchange casing") {
  REQUIRE(etf::side_from_string("buy") == etf::Side::Buy);
  REQUIRE(etf::side_from_string("Buy") == etf::Side::Buy);
  REQUIRE(etf::side_from_string("sell") == etf::Side::Sell);
  REQUIRE(etf::side_from_string("Sell") == etf::Side::Sell);
}

TEST_CASE("config round-trip preserves advisory mode") {
  const auto path = std::filesystem::temp_directory_path() / "etf_advisory_config.json";
  {
    std::ofstream out(path);
    out << R"({
      "simulation": {},
      "strategy": { "name": "challenge_v1" },
      "run": {
        "advisory_only": true,
        "live": { "enabled": true, "access_token": "token" }
      }
    })";
  }

  const auto config = etf::load_config_from_path(path.string());
  REQUIRE(config.run.advisory_only);
  REQUIRE(config.run.live.enabled);
  REQUIRE(etf::config_to_json(config).at("run").at("advisory_only").get<bool>());

  std::filesystem::remove(path);
}
