#pragma once

#include <optional>
#include <string>
#include <vector>

#include "etf/core.hpp"

namespace etf {

std::string exchange_login_url(const LiveConfig& live);
std::string exchange_me_url(const LiveConfig& live);
std::string exchange_websocket_url(const LiveConfig& live);

nlohmann::json exchange_request_state_message(const LiveConfig& live);
nlohmann::json exchange_mark_ready_message(const LiveConfig& live);
nlohmann::json exchange_place_order_message(const LiveConfig& live, Symbol market, Side side,
                                            PriceTick price, Qty size);
nlohmann::json exchange_cancel_order_message(const LiveConfig& live, OrderId order_id);
nlohmann::json exchange_cancel_all_orders_message(const LiveConfig& live,
                                                  std::optional<Symbol> market = std::nullopt);
nlohmann::json exchange_subscribe_book_message(const LiveConfig& live, Symbol market,
                                               std::optional<TimestampUs> since = std::nullopt);
nlohmann::json exchange_unsubscribe_book_message(const LiveConfig& live, Symbol market);

std::vector<std::string> exchange_client_actions();
std::vector<std::string> exchange_update_types();
std::vector<std::string> exchange_subtype_update_types();
std::string exchange_protocol_summary(const LiveConfig& live);

}  // namespace etf
