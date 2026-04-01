#pragma once

#include <fstream>
#include <string>

#include "etf/core.hpp"

namespace etf {

class RunLogger {
 public:
  RunLogger() = default;
  explicit RunLogger(const std::string& path);
  ~RunLogger();

  void open(const std::string& path);
  bool is_open() const;
  void log_config(const AppConfig& config);
  void log_market_event(const MarketEvent& event);
  void log_order_command(TimestampUs ts, const OrderCommand& command, const nlohmann::json& extra = {});
  void log_decision(TimestampUs ts, const std::string& strategy_name, const nlohmann::json& diagnostics);
  void log_snapshot(const MarketSnapshot& snapshot);
  void log_summary(const SummaryStats& summary);

 private:
  std::ofstream out_;
};

SummaryStats simulate_to_log(const AppConfig& config);
SummaryStats replay_to_log(const AppConfig& config);
SummaryStats live_to_log(const AppConfig& config);

}  // namespace etf
