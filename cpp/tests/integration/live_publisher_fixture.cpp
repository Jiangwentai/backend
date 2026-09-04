#include "market_data/dispatcher.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/synthetic_generator.hpp"
#include "market_data/zmq_publisher.hpp"
#include <chrono>
#include <thread>
int main(int argc,char**argv){using namespace std::chrono_literals;std::string endpoint=argc>1?argv[1]:"tcp://127.0.0.1:15556";market_data::SpscQueue<market_data::MarketTick>ingress(4096),persistence(4096);market_data::LiveQueue live(4096);market_data::ProducerIdentity id;market_data::Dispatcher dispatcher(ingress,persistence,&live);market_data::ZmqPublisher publisher(live,endpoint);publisher.start();dispatcher.start();std::this_thread::sleep_for(300ms);market_data::SyntheticGenerator generator(ingress,id,1000,{"SHFE.zn2610","SHFE.cu2610"});generator.start();while(generator.generated()<40){market_data::MarketTick t;while(persistence.try_pop(t)){}std::this_thread::sleep_for(1ms);}generator.request_stop();generator.join();dispatcher.request_stop();dispatcher.join();publisher.request_stop();publisher.join();return 0;}
