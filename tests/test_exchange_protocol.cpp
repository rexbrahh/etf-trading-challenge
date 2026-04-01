#include <catch2/catch_test_macros.hpp>

#include "etf/exchange_protocol.hpp"

TEST_CASE("exchange protocol derives cookie-backed HTTP and websocket endpoints") {
  etf::LiveConfig live;
  live.api_base_url = "https://api.comp.waterlooquantclub.com";
  REQUIRE(etf::exchange_login_url(live) == "https://api.comp.waterlooquantclub.com/auth/user/login");
  REQUIRE(etf::exchange_me_url(live) == "https://api.comp.waterlooquantclub.com/auth/me");
  REQUIRE(etf::exchange_websocket_url(live) == "wss://api.comp.waterlooquantclub.com/ws");
}

TEST_CASE("exchange protocol builds exact websocket envelopes for competitor actions") {
  etf::LiveConfig live;
  live.run_id = "a6f5e8ee-4597-485f-aa17-b90737d7acc5";

  REQUIRE(etf::exchange_request_state_message(live) ==
          nlohmann::json{{"type", "RequestState"}, {"run_id", live.run_id}});

  REQUIRE(etf::exchange_mark_ready_message(live) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data", {{"action", "MarkReady"}}}});

  REQUIRE(etf::exchange_place_order_message(live, etf::Symbol::ETF, etf::Side::Buy, 501, 10) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data",
                          {{"action", "PlaceOrder"},
                           {"market", "ETF"},
                           {"side", "Buy"},
                           {"price", 501},
                           {"size", 10}}}});

  REQUIRE(etf::exchange_cancel_order_message(live, 42) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data", {{"action", "CancelOrder"}, {"order_id", 42}}}});

  REQUIRE(etf::exchange_cancel_all_orders_message(live, etf::Symbol::F) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data", {{"action", "CancelAllOrders"}, {"market", "F"}}}});

  REQUIRE(etf::exchange_subscribe_book_message(live, etf::Symbol::E, 123456) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data", {{"action", "SubscribeToBook"}, {"market", "E"}, {"since", 123456}}}});

  REQUIRE(etf::exchange_unsubscribe_book_message(live, etf::Symbol::T) ==
          nlohmann::json{{"type", "GameAction"},
                         {"run_id", live.run_id},
                         {"game", "Exchange"},
                         {"data", {{"action", "UnsubscribeFromBook"}, {"market", "T"}}}});
}

TEST_CASE("exchange protocol summary lists discovered actions and payload types") {
  etf::LiveConfig live;
  live.run_id = "run-123";
  const auto summary = etf::exchange_protocol_summary(live);
  REQUIRE(summary.find("MarkReady") != std::string::npos);
  REQUIRE(summary.find("PlaceOrder") != std::string::npos);
  REQUIRE(summary.find("UserStateUpdate") != std::string::npos);
  REQUIRE(summary.find("PolymarketRoll") != std::string::npos);
  REQUIRE(summary.find("run-123") != std::string::npos);
}
