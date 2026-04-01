#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "etf/analysis.hpp"
#include "etf/core.hpp"
#include "etf/exchange_protocol.hpp"
#include "etf/logging.hpp"

namespace {

std::string resolve_config_path(const std::string& raw) {
  if (std::filesystem::exists(raw)) {
    return raw;
  }
  if (std::filesystem::exists("../" + raw)) {
    return "../" + raw;
  }
  throw std::runtime_error("config not found: " + raw);
}

void print_usage() {
  std::cout << "usage: etf_lab <simulate|replay|sweep|analyze|live|advise|live-protocol> <config-or-log>\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 3) {
      print_usage();
      return 1;
    }

    const std::string command = argv[1];
    const std::string path = argv[2];

    if (command == "simulate") {
      auto config = etf::load_config_from_path(resolve_config_path(path));
      const auto summary = etf::simulate_to_log(config);
      etf::print_summary(summary);
      return 0;
    }
    if (command == "replay") {
      auto config = etf::load_config_from_path(resolve_config_path(path));
      const auto summary = etf::replay_to_log(config);
      etf::print_summary(summary);
      return 0;
    }
    if (command == "sweep") {
      auto config = etf::load_config_from_path(resolve_config_path(path));
      etf::sweep_configs(config);
      return 0;
    }
    if (command == "analyze") {
      const auto summary = etf::analyze_log(resolve_config_path(path));
      etf::print_summary(summary);
      return 0;
    }
    if (command == "live") {
      auto config = etf::load_config_from_path(resolve_config_path(path));
      if (!config.run.live.enabled) {
        throw std::runtime_error("run.live.enabled must be true for `live`");
      }
      const auto summary = etf::live_to_log(config);
      etf::print_summary(summary);
      return 0;
    }
    if (command == "advise") {
      auto config = etf::load_config_from_path(resolve_config_path(path));
      if (!config.run.live.enabled) {
        throw std::runtime_error("run.live.enabled must be true for `advise`");
      }
      config.run.advisory_only = true;
      std::cout << "advisory mode armed; waiting for the next live trading round...\n";
      const auto summary = etf::live_to_log(config);
      etf::print_summary(summary);
      return 0;
    }
    if (command == "live-protocol") {
      const auto config = etf::load_config_from_path(resolve_config_path(path));
      std::cout << etf::exchange_protocol_summary(config.run.live);
      return 0;
    }

    print_usage();
    return 1;
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << '\n';
    return 1;
  }
}
