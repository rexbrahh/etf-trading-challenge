#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "etf/core.hpp"
#include "etf/gateway.hpp"
#include "etf/strategy.hpp"

namespace {

std::string resolve_default_config() {
  if (std::filesystem::exists("configs/default.json")) {
    return "configs/default.json";
  }
  if (std::filesystem::exists("../configs/default.json")) {
    return "../configs/default.json";
  }
  throw std::runtime_error("configs/default.json not found");
}

double percentile(std::vector<double> values, double pct) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  const auto index = static_cast<std::size_t>(pct * static_cast<double>(values.size() - 1));
  return values[index];
}

}  // namespace

int main() {
  const auto config_path = resolve_default_config();
  auto base = etf::load_config_from_path(config_path);
  base.run.log_output_path.clear();
  base.simulation.duration_us = 120'000;

  const std::vector<std::string> strategies{"basket_arb", "event_prop", "hybrid", "challenge_v1"};
  const int rounds = 1000;
  const auto start = std::chrono::steady_clock::now();

  for (const auto& strategy_name : strategies) {
    std::vector<double> pnls;
    std::map<std::string, int> fill_breakdown;
    for (int i = 0; i < rounds; ++i) {
      auto config = base;
      config.strategy.name = strategy_name;
      config.simulation.seed = static_cast<std::uint64_t>(1000 + i);
      auto gateway = etf::make_sim_gateway(config);
      auto strategy = etf::make_strategy(config.strategy);
      etf::SummaryStats summary;
      etf::run_gateway_session(*gateway, *strategy, config, &summary, nullptr);
      pnls.push_back(summary.pnl.total);
      for (const auto& [bucket, qty] : summary.fill_breakdown) {
        fill_breakdown[bucket] += qty;
      }
    }

    const auto mean =
        std::accumulate(pnls.begin(), pnls.end(), 0.0) / static_cast<double>(pnls.size());
    auto sorted = pnls;
    std::sort(sorted.begin(), sorted.end());
    const auto median = sorted[sorted.size() / 2];
    const auto downside_tail = percentile(sorted, 0.05);

    std::cout << "strategy=" << strategy_name << " rounds=" << rounds << " mean_pnl=" << mean
              << " median_pnl=" << median << " p05_pnl=" << downside_tail << '\n';
    for (const auto& [bucket, qty] : fill_breakdown) {
      std::cout << "  fill_breakdown " << bucket << "=" << qty << '\n';
    }
  }

  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
  std::cout << "elapsed_ms=" << elapsed_ms << '\n';
  return 0;
}
