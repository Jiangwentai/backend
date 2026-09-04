cmake_minimum_required(VERSION 3.25)
project(financial_market_data_backend VERSION 0.1.0 LANGUAGES C CXX)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(MD_BUILD_TESTS "Build unit and integration tests" ON)
option(MD_BUILD_BENCHMARKS "Build synthetic benchmark" ON)
option(ENABLE_CTP "Build the operator-supplied CTP market-data adapter" OFF)

include(cmake/Dependencies.cmake)
find_package(Threads REQUIRED)

add_library(market_data_core
  cpp/src/config.cpp
  cpp/src/logging.cpp
  cpp/src/market_tick.cpp
  cpp/src/provider.cpp
  cpp/src/provider_manager.cpp
  cpp/src/instrument_mapping.cpp
  cpp/src/producer_identity.cpp
  cpp/src/dispatcher.cpp
  cpp/src/live_protocol.cpp
  cpp/src/zmq_publisher.cpp
  cpp/src/synthetic_generator.cpp
  cpp/src/questdb_writer.cpp
  cpp/src/pipeline.cpp
  cpp/src/ctp/normalizer.cpp
  cpp/src/ctp/state_machine.cpp)
target_include_directories(market_data_core PUBLIC cpp/include)
target_link_libraries(market_data_core PUBLIC Threads::Threads spdlog::spdlog yaml-cpp::yaml-cpp questdb_client cppzmq msgpack-cxx)
target_compile_options(market_data_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

if(ENABLE_CTP)
  find_package(CTP REQUIRED)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_INCLUDES "${CTP_INCLUDE_DIR}")
  check_cxx_source_compiles([[#include <ThostFtdcMdApi.h>
int main() {
  CThostFtdcMdApi* api = nullptr;
  CThostFtdcReqAuthenticateField request{};
  return api->ReqAuthenticate(&request, 1);
}]] MD_CTP_HAS_AUTHENTICATE)
  unset(CMAKE_REQUIRED_INCLUDES)
  target_sources(market_data_core PRIVATE cpp/src/ctp/adapter.cpp)
  target_link_libraries(market_data_core PRIVATE CTP::MdApi)
  target_compile_definitions(market_data_core PUBLIC MD_ENABLE_CTP=1)
  if(MD_CTP_HAS_AUTHENTICATE)
    target_compile_definitions(market_data_core PRIVATE MD_CTP_HAS_AUTHENTICATE=1)
  endif()
endif()

add_executable(market_data_collector cpp/src/main.cpp)
target_link_libraries(market_data_collector PRIVATE market_data_core)
set_target_properties(market_data_collector PROPERTIES BUILD_RPATH_USE_ORIGIN TRUE)

if(MD_BUILD_TESTS)
  enable_testing()
  add_executable(market_data_unit_tests
    cpp/tests/unit/test_market_tick.cpp
    cpp/tests/unit/test_spsc_queue.cpp
    cpp/tests/unit/test_dispatcher.cpp
    cpp/tests/unit/test_live_queue.cpp
    cpp/tests/unit/test_live_protocol.cpp
    cpp/tests/unit/test_batch_policy.cpp
    cpp/tests/unit/test_config.cpp
    cpp/tests/unit/test_ctp_normalizer.cpp
    cpp/tests/unit/test_ctp_state_machine.cpp
    cpp/tests/unit/test_ctp_simulated_pipeline.cpp)
  target_sources(market_data_unit_tests PRIVATE cpp/tests/unit/test_provider_architecture.cpp)
  target_link_libraries(market_data_unit_tests PRIVATE market_data_core GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(market_data_unit_tests PROPERTIES LABELS unit)

  add_executable(questdb_integration_tests cpp/tests/integration/test_questdb.cpp)
  target_link_libraries(questdb_integration_tests PRIVATE market_data_core GTest::gtest_main)
  add_test(NAME questdb_integration_tests COMMAND questdb_integration_tests)
  set_tests_properties(questdb_integration_tests PROPERTIES LABELS integration)
  add_executable(live_publisher_fixture cpp/tests/integration/live_publisher_fixture.cpp)
  target_link_libraries(live_publisher_fixture PRIVATE market_data_core)
  add_test(NAME python_live_tests COMMAND ${CMAKE_COMMAND} -E env PYTHONPATH=${CMAKE_SOURCE_DIR}/python LIVE_FIXTURE=$<TARGET_FILE:live_publisher_fixture> python3 -m pytest -q ${CMAKE_SOURCE_DIR}/python/tests)
  set_tests_properties(python_live_tests PROPERTIES LABELS live)
endif()

if(MD_BUILD_BENCHMARKS)
  add_executable(synthetic_throughput cpp/benchmarks/synthetic_throughput.cpp)
  target_link_libraries(synthetic_throughput PRIVATE market_data_core)
  add_executable(combined_throughput cpp/benchmarks/combined_throughput.cpp)
  target_link_libraries(combined_throughput PRIVATE market_data_core)
endif()
