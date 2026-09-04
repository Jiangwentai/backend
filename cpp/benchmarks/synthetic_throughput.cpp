#include "market_data/market_tick.hpp"
#include "market_data/producer_identity.hpp"
#include "market_data/spsc_queue.hpp"
#include <chrono>
#include <iostream>
#include <thread>
int main(int argc,char**argv){const std::uint64_t count=argc>1?std::stoull(argv[1]):1'000'000;market_data::SpscQueue<market_data::MarketTick>q(1'048'576);market_data::ProducerIdentity id;auto start=std::chrono::steady_clock::now();std::thread consumer([&]{market_data::MarketTick t;std::uint64_t n=0;while(n<count){if(q.try_pop(t))++n;else std::this_thread::yield();}});for(std::uint64_t i=0;i<count;++i){market_data::MarketTick t;t.producer_id=id.id();t.seq=id.next_seq();while(!q.try_push(t))std::this_thread::yield();}consumer.join();double s=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();std::cout<<"ticks="<<count<<" seconds="<<s<<" ticks_per_second="<<static_cast<double>(count)/s<<'\n';return 0;}

