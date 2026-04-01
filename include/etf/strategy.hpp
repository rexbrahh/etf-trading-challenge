#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "etf/core.hpp"

namespace etf {

struct StrategyDecision {
  std::vector<OrderCommand> commands;
  nlohmann::json diagnostics = nlohmann::json::object();
};

class Strategy {
 public:
  virtual ~Strategy() = default;
  virtual std::string name() const = 0;
  virtual StrategyDecision on_start(const MarketSnapshot& snapshot) = 0;
  virtual StrategyDecision on_event(const MarketEvent& event, const MarketSnapshot& snapshot) = 0;
  virtual StrategyDecision on_timer(const Timer& timer, const MarketSnapshot& snapshot) = 0;
  virtual StrategyDecision on_end(const MarketSnapshot& snapshot) = 0;
};

std::unique_ptr<Strategy> make_strategy(const StrategyConfig& config);

}  // namespace etf
