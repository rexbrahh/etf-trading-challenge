#include "etf/analysis.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "etf/logging.hpp"

namespace etf {

namespace {

void apply_override(AppConfig& config, const std::string& path, const nlohmann::json& value) {
  if (path == "simulation.seed") {
    config.simulation.seed = value.get<std::uint64_t>();
  } else if (path == "simulation.duration_us") {
    config.simulation.duration_us = value.get<TimestampUs>();
  } else if (path == "simulation.enable_passive_opponent") {
    config.simulation.enable_passive_opponent = value.get<bool>();
  } else if (path == "simulation.enable_naive_arb_opponent") {
    config.simulation.enable_naive_arb_opponent = value.get<bool>();
  } else if (path == "strategy.edge_buffer_ticks") {
    config.strategy.edge_buffer_ticks = value.get<int>();
  } else if (path == "strategy.quote_width_ticks") {
    config.strategy.quote_width_ticks = value.get<int>();
  } else if (path == "strategy.event_threshold_qty") {
    config.strategy.event_threshold_qty = value.get<Qty>();
  } else if (path == "strategy.competition_share") {
    config.strategy.competition_share = value.get<double>();
  } else if (path == "strategy.name") {
    config.strategy.name = value.get<std::string>();
  } else {
    throw std::invalid_argument("unsupported sweep override path: " + path);
  }
}

void run_sweep_recursive(const AppConfig& base, const nlohmann::json& overrides, std::size_t index,
                         std::vector<std::pair<std::string, nlohmann::json>>& active) {
  if (index == overrides.size()) {
    auto config = base;
    std::string suffix;
    for (const auto& [path, value] : active) {
      apply_override(config, path, value);
      suffix += "_" + path.substr(path.find('.') + 1) + "_" + value.dump();
    }
    std::replace(suffix.begin(), suffix.end(), '"', '_');
    std::replace(suffix.begin(), suffix.end(), '.', '_');
    config.run.log_output_path = "tmp/sweeps/" + config.strategy.name + suffix + ".ndjson";
    const auto summary = simulate_to_log(config);
    std::cout << "sweep " << config.run.log_output_path << " -> pnl " << summary.pnl.total << '\n';
    return;
  }

  const auto& item = overrides.at(index);
  const auto path = item.at("path").get<std::string>();
  for (const auto& value : item.at("values")) {
    active.emplace_back(path, value);
    run_sweep_recursive(base, overrides, index + 1, active);
    active.pop_back();
  }
}

}  // namespace

SummaryStats analyze_log(const std::string& path) {
  std::ifstream in(path);
  if (!in.is_open()) {
    throw std::runtime_error("unable to open log: " + path);
  }

  SummaryStats summary;
  std::unordered_map<OrderId, std::string> order_tags;
  std::unordered_map<OrderId, bool> stale_order_ids;
  std::vector<double> event_latencies;
  std::optional<TimestampUs> latest_event_burst;

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    const auto json = nlohmann::json::parse(line);
    const auto kind = json.at("kind").get<std::string>();
    if (kind == "summary") {
      const auto& node = json.at("summary");
      summary.strategy_name = node.at("strategy_name").get<std::string>();
      summary.seed = node.at("seed").get<std::uint64_t>();
      summary.pnl.realized = node.at("pnl").at("realized").get<double>();
      summary.pnl.unrealized = node.at("pnl").at("unrealized").get<double>();
      summary.pnl.total = node.at("pnl").at("total").get<double>();
      summary.arb_orders = node.at("arb_orders").get<int>();
      summary.arb_fills = node.at("arb_fills").get<int>();
      summary.stale_quote_fills = node.at("stale_quote_fills").get<int>();
      summary.event_orders = node.at("event_orders").get<int>();
    } else if (kind == "order_command") {
      const auto& command = json.at("command");
      const auto type = command.at("type").get<std::string>();
      if (type == "place_limit") {
        const auto id = command.at("order_id").get<OrderId>();
        const auto tag = command.at("tag").get<std::string>();
        order_tags[id] = tag;
        if (tag.find("basket_arb") != std::string::npos) {
          summary.arb_orders += 1;
        }
        if (tag.find("event_reprice") != std::string::npos) {
          summary.event_orders += 1;
          if (latest_event_burst) {
            event_latencies.push_back(static_cast<double>(json.at("ts").get<TimestampUs>() - *latest_event_burst));
            latest_event_burst.reset();
          }
        }
      } else if (type == "cancel" && command.at("reason").get<std::string>() == "stale_cancel") {
        stale_order_ids[command.at("order_id").get<OrderId>()] = true;
      }
    } else if (kind == "market_event") {
      const auto& event = json.at("event");
      const auto type = event.at("type").get<std::string>();
      if (type == "trade_print" && event.at("source").get<std::string>() == "event_bot" &&
          event.at("symbol").get<std::string>() != "ETF") {
        latest_event_burst = event.at("ts").get<TimestampUs>();
      } else if (type == "fill") {
        const auto id = event.at("order_id").get<OrderId>();
        const auto tag_it = order_tags.find(id);
        if (tag_it != order_tags.end()) {
          if (tag_it->second.find("basket_arb") != std::string::npos) {
            summary.arb_fills += 1;
            summary.fill_breakdown["basket_arb"] += event.at("qty").get<int>();
          }
          if (tag_it->second.find("event_reprice") != std::string::npos) {
            summary.fill_breakdown["event_reprice"] += event.at("qty").get<int>();
          }
        }
        if (stale_order_ids.contains(id)) {
          summary.stale_quote_fills += 1;
        }
      }
    } else if (kind == "snapshot") {
      const auto& positions = json.at("snapshot").at("positions");
      for (const auto& symbol : all_symbols()) {
        summary.final_positions[symbol] = positions.at(to_string(symbol)).at("qty").get<int>();
      }
      summary.pnl.realized = json.at("snapshot").at("pnl").at("realized").get<double>();
      summary.pnl.unrealized = json.at("snapshot").at("pnl").at("unrealized").get<double>();
      summary.pnl.total = json.at("snapshot").at("pnl").at("total").get<double>();
    }
  }

  if (!event_latencies.empty()) {
    summary.avg_event_latency_us =
        std::accumulate(event_latencies.begin(), event_latencies.end(), 0.0) / event_latencies.size();
  }
  return summary;
}

void sweep_configs(const AppConfig& config) {
  if (!config.run.sweep_overrides.is_array() || config.run.sweep_overrides.empty()) {
    throw std::invalid_argument("run.sweep_overrides must be a non-empty array");
  }
  std::vector<std::pair<std::string, nlohmann::json>> active;
  run_sweep_recursive(config, config.run.sweep_overrides, 0, active);
}

void print_summary(const SummaryStats& summary) {
  std::cout << "strategy: " << summary.strategy_name << '\n';
  std::cout << "seed: " << summary.seed << '\n';
  std::cout << "pnl total: " << std::fixed << std::setprecision(2) << summary.pnl.total << '\n';
  std::cout << "pnl realized: " << summary.pnl.realized << '\n';
  std::cout << "pnl unrealized: " << summary.pnl.unrealized << '\n';
  std::cout << "arb orders/fills: " << summary.arb_orders << "/" << summary.arb_fills << '\n';
  std::cout << "event orders: " << summary.event_orders << '\n';
  std::cout << "stale quote fills: " << summary.stale_quote_fills << '\n';
  std::cout << "avg event latency (us): " << summary.avg_event_latency_us << '\n';
  for (const auto& [symbol, qty] : summary.final_positions) {
    std::cout << "final position " << to_string(symbol) << ": " << qty << '\n';
  }
  for (const auto& [bucket, qty] : summary.fill_breakdown) {
    std::cout << "fill breakdown " << bucket << ": " << qty << '\n';
  }
}

}  // namespace etf
