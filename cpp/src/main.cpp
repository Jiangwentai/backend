#include "market_data/config.hpp"
#include "market_data/logging.hpp"
#include "market_data/pipeline.hpp"
#include <atomic>
#include <csignal>
#include <chrono>
#include <iostream>
#include <thread>
namespace {std::atomic<bool> stop{false};extern "C" void signal_handler(int){stop=true;}}
int main(int argc,char**argv){try{auto config=market_data::load_config(argc>1?argv[1]:"config/app.yaml");market_data::init_logging(config.log_level);std::signal(SIGINT,signal_handler);std::signal(SIGTERM,signal_handler);market_data::Pipeline pipeline{std::move(config)};pipeline.start();while(!stop)std::this_thread::sleep_for(std::chrono::milliseconds(100));pipeline.shutdown();return 0;}catch(const std::exception&e){std::cerr<<e.what()<<'\n';return 1;}}
