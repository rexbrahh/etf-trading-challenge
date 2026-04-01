#include "etf/exchange_protocol.hpp"

#include <sstream>
#include <stdexcept>

namespace etf {

namespace {

std::string require_run_id(const LiveConfig& live) {
  if (live.run_id.empty()) {
    throw std::invalid_argument("run.live.run_id is required for exchange protocol messages");
  }
  return live.run_id;
}

nlohmann::json exchange_game_action(const LiveConfig& live, const nlohmann::json& data) {
  return {
      {"type", "GameAction"},
      {"run_id", require_run_id(live)},
      {"game", "Exchange"},
      {"data", data},
  };
}

std::string join_with_comma(const std::vector<std::string>& items) {
  std::ostringstream out;
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << items[i];
  }
  return out.str();
}

}  // namespace

std::string exchange_login_url(const LiveConfig& live) { return live.api_base_url + "/auth/user/login"; }

std::string exchange_me_url(const LiveConfig& live) { return live.api_base_url + "/auth/me"; }

std::string exchange_websocket_url(const LiveConfig& live) {
  if (!live.websocket_url.empty()) {
    return live.websocket_url;
  }
  auto url = live.api_base_url;
  if (url.rfind("https://", 0) == 0) {
    url.replace(0, 5, "wss");
  } else if (url.rfind("http://", 0) == 0) {
    url.replace(0, 4, "ws");
  }
  return url + "/ws";
}

nlohmann::json exchange_request_state_message(const LiveConfig& live) {
  return {
      {"type", "RequestState"},
      {"run_id", require_run_id(live)},
  };
}

nlohmann::json exchange_mark_ready_message(const LiveConfig& live) {
  return exchange_game_action(live, {{"action", "MarkReady"}});
}

nlohmann::json exchange_place_order_message(const LiveConfig& live, Symbol market, Side side, PriceTick price,
                                            Qty size) {
  return exchange_game_action(
      live, {{"action", "PlaceOrder"},
             {"market", to_string(market)},
             {"side", side == Side::Buy ? "Buy" : "Sell"},
             {"price", price},
             {"size", size}});
}

nlohmann::json exchange_cancel_order_message(const LiveConfig& live, OrderId order_id) {
  return exchange_game_action(live, {{"action", "CancelOrder"}, {"order_id", order_id}});
}

nlohmann::json exchange_cancel_all_orders_message(const LiveConfig& live, std::optional<Symbol> market) {
  return exchange_game_action(live, {{"action", "CancelAllOrders"},
                                     {"market", market ? nlohmann::json(to_string(*market))
                                                       : nlohmann::json(nullptr)}});
}

nlohmann::json exchange_subscribe_book_message(const LiveConfig& live, Symbol market,
                                               std::optional<TimestampUs> since) {
  return exchange_game_action(live, {{"action", "SubscribeToBook"},
                                     {"market", to_string(market)},
                                     {"since", since ? nlohmann::json(*since) : nlohmann::json(nullptr)}});
}

nlohmann::json exchange_unsubscribe_book_message(const LiveConfig& live, Symbol market) {
  return exchange_game_action(live, {{"action", "UnsubscribeFromBook"}, {"market", to_string(market)}});
}

std::vector<std::string> exchange_client_actions() {
  return {"MarkReady", "PlaceOrder", "CancelOrder", "CancelAllOrders", "SubscribeToBook",
          "UnsubscribeFromBook"};
}

std::vector<std::string> exchange_update_types() {
  return {"StateChange", "OrderBookUpdate", "ReadyAcknowledged", "UserStateUpdate", "BookSubscribed",
          "BookUnsubscribed", "NewTrades", "PnlUpdate", "PriceUpdate", "SubtypeUpdate"};
}

std::vector<std::string> exchange_subtype_update_types() { return {"AbcDraw", "PolymarketRoll"}; }

std::string exchange_protocol_summary(const LiveConfig& live) {
  std::ostringstream out;
  out << "Login endpoint: " << exchange_login_url(live) << '\n';
  out << "Current-user endpoint: " << exchange_me_url(live) << '\n';
  out << "WebSocket endpoint: " << exchange_websocket_url(live) << '\n';
  out << "Run ID: " << (live.run_id.empty() ? "<unset>" : live.run_id) << '\n';
  out << "Client actions: " << join_with_comma(exchange_client_actions()) << '\n';
  out << "GameUpdate payload types: " << join_with_comma(exchange_update_types()) << '\n';
  out << "SubtypeUpdate payload types: " << join_with_comma(exchange_subtype_update_types()) << '\n';
  out << "Auth flow: POST /auth/user/login with {\"access_token\": ...}, then reuse the session cookie for "
         "REST and WebSocket requests.\n";
  return out.str();
}

}  // namespace etf
