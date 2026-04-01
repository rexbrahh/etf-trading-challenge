cmake_minimum_required(VERSION 3.21)

project(etf_trading_challenge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

FetchContent_Declare(
  nlohmann_json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)

FetchContent_Declare(
  Catch2
  URL https://github.com/catchorg/Catch2/archive/refs/tags/v3.8.1.tar.gz
)

FetchContent_MakeAvailable(nlohmann_json Catch2)

add_library(
  etf_lib
  src/core.cpp
  src/exchange_protocol.cpp
  src/strategy.cpp
  src/gateway.cpp
  src/simulator.cpp
  src/logging.cpp
  src/analysis.cpp
)

target_include_directories(etf_lib PUBLIC include)
target_link_libraries(etf_lib PUBLIC nlohmann_json::nlohmann_json)
target_compile_features(etf_lib PUBLIC cxx_std_20)
target_compile_options(etf_lib PRIVATE -Wall -Wextra -Wpedantic)

add_executable(etf_lab tools/main.cpp)
target_link_libraries(etf_lab PRIVATE etf_lib)

add_executable(etf_bench_smoke tools/bench_smoke.cpp)
target_link_libraries(etf_bench_smoke PRIVATE etf_lib)

add_executable(
  etf_tests
  tests/test_core.cpp
  tests/test_exchange_protocol.cpp
  tests/test_simulator.cpp
  tests/test_strategies.cpp
)

target_link_libraries(etf_tests PRIVATE etf_lib Catch2::Catch2WithMain)

include(CTest)
include(Catch)
catch_discover_tests(etf_tests)
add_test(NAME bench_smoke COMMAND etf_bench_smoke)
