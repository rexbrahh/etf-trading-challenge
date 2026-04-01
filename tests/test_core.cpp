#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

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
