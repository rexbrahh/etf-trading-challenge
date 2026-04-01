#pragma once

#include <string>

#include "etf/core.hpp"

namespace etf {

SummaryStats analyze_log(const std::string& path);
void sweep_configs(const AppConfig& config);
void print_summary(const SummaryStats& summary);

}  // namespace etf
