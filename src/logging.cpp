#include "etf/logging.hpp"

#include <filesystem>
#include <iostream>

#include "etf/gateway.hpp"
#include "etf/strategy.hpp"

namespace etf {

namespace {

void write_json_line(std::ofstream& out, const nlohmann::json& json) {
  out << json.dump() << '\n';
}

}  // namespace

RunLogger::RunLogger(const std::string& path) { open(path); }

RunLogger::~RunLogger() {
  if (out_.is_open()) {
    out_.flush();
  }
}

void RunLogger::open(const std::string& path) {
  std::filesystem::path output(path);
  if (output.has_parent_path()) {
    std::filesystem::create_directories(output.parent_path());
  }
  out_.open(path, std::ios::out | std::ios::trunc);
  if (!out_.is_open()) {
    throw std::runtime_error("unable to open log file: " + path);
  }
}

bool RunLogger::is_open() const { return out_.is_open(); }

void RunLogger::log_config(const AppConfig& config) {
  if (!is_open()) {
    return;
  }
  write_json_line(out_, {{"kind", "config"}, {"config", config_to_json(config)}});
}

void RunLogger::log_market_event(const MarketEvent& event) {
  if (!is_open()) {
    return;
  }
  write_json_line(out_, {{"kind", "market_event"}, {"event", event_to_json(event)}});
}

void RunLogger::log_order_command(TimestampUs ts, const OrderCommand& command, const nlohmann::json& extra) {
  if (!is_open()) {
    return;
  }
  nlohmann::json line = {{"kind", "order_command"}, {"ts", ts}, {"command", command_to_json(command)}};
  for (auto it = extra.begin(); it != extra.end(); ++it) {
    line[it.key()] = it.value();
  }
  write_json_line(out_, line);
}

void RunLogger::log_decision(TimestampUs ts, const std::string& strategy_name,
                             const nlohmann::json& diagnostics) {
  if (!is_open()) {
    return;
  }
  write_json_line(out_, {{"kind", "strategy_decision"},
                         {"ts", ts},
                         {"strategy_name", strategy_name},
                         {"diagnostics", diagnostics}});
}

void RunLogger::log_snapshot(const MarketSnapshot& snapshot) {
  if (!is_open()) {
    return;
  }
  write_json_line(out_, {{"kind", "snapshot"}, {"snapshot", snapshot_to_json(snapshot)}});
}

void RunLogger::log_summary(const SummaryStats& summary) {
  if (!is_open()) {
    return;
  }
  write_json_line(out_, {{"kind", "summary"}, {"summary", summary_to_json(summary)}});
}

SummaryStats simulate_to_log(const AppConfig& config) {
  RunLogger logger(config.run.log_output_path);
  auto strategy = make_strategy(config.strategy);
  auto gateway = make_sim_gateway(config, &logger);
  SummaryStats summary;
  run_gateway_session(*gateway, *strategy, config, &summary, &logger);
  logger.log_summary(summary);
  return summary;
}

SummaryStats replay_to_log(const AppConfig& config) {
  RunLogger logger(config.run.log_output_path);
  auto strategy = make_strategy(config.strategy);
  auto gateway = make_replay_gateway(config, &logger);
  SummaryStats summary;
  run_gateway_session(*gateway, *strategy, config, &summary, &logger);
  logger.log_summary(summary);
  return summary;
}

SummaryStats live_to_log(const AppConfig& config) {
  RunLogger logger(config.run.log_output_path);
  auto strategy = make_strategy(config.strategy);
  auto gateway = make_live_gateway(config, &logger);
  SummaryStats summary;
  run_gateway_session(*gateway, *strategy, config, &summary, &logger);
  logger.log_summary(summary);
  return summary;
}

}  // namespace etf
