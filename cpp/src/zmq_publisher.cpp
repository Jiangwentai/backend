#include "market_data/zmq_publisher.hpp"
#include "market_data/live_protocol.hpp"
#include <array>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include <spdlog/spdlog.h>
namespace market_data {ZmqPublisher::~ZmqPublisher(){request_stop();join();}void ZmqPublisher::start(){thread_=std::thread(&ZmqPublisher::run,this);}void ZmqPublisher::request_stop()noexcept{stop_=true;}void ZmqPublisher::join(){if(thread_.joinable())thread_.join();}void ZmqPublisher::run(){try{zmq::context_t context{1};zmq::socket_t socket{context,zmq::socket_type::pub};socket.set(zmq::sockopt::linger,0);socket.bind(endpoint_);MarketTick t;while(!stop_||!queue_.empty()){if(!queue_.try_pop(t)){std::this_thread::yield();continue;}auto topic=live_topic(t);auto body=encode_live_tick(t);const std::array<zmq::const_buffer,2>frames{zmq::buffer(topic),zmq::buffer(body)};try{const auto result=zmq::send_multipart(socket,frames,zmq::send_flags::dontwait);if(result&&*result==frames.size())++sent_;else{++failures_;spdlog::warn("zeromq_multipart_send_would_block");}}catch(const zmq::error_t&e){++failures_;spdlog::error("zeromq_multipart_send_failure error={}",e.what());throw;}}}catch(const std::exception&e){++failures_;spdlog::critical("zeromq_publisher_failure error={}",e.what());}}}
