#pragma once

#include <memory>
#include <string>
#include <vector>

#include "etf/core.hpp"

namespace etf {

class RunLogger;
class Strategy;

class MarketGateway {
 public:
  virtual ~MarketGateway() = default;
  virtual void connect() = 0;
  virtual std::vector<MarketEvent> poll() = 0;
  virtual void submit(const std::vector<OrderCommand>& commands) = 0;
  virtual void flush() = 0;
  virtual void disconnect() = 0;
};

class Simulator;

std::unique_ptr<MarketGateway> make_sim_gateway(const AppConfig& config);
std::unique_ptr<MarketGateway> make_sim_gateway(const AppConfig& config, RunLogger* logger);
std::unique_ptr<MarketGateway> make_replay_gateway(const AppConfig& config);
std::unique_ptr<MarketGateway> make_replay_gateway(const AppConfig& config, RunLogger* logger);
std::unique_ptr<MarketGateway> make_web_gateway_stub(const AppConfig& config);

MarketSnapshot run_gateway_session(MarketGateway& gateway, Strategy& strategy, AppConfig config,
                                   SummaryStats* summary = nullptr, RunLogger* logger = nullptr);

}  // namespace etf
