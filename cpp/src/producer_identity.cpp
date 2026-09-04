#include "market_data/producer_identity.hpp"
#include <array>
#include <cstdio>
#include <random>
namespace market_data {
ProducerIdentity::ProducerIdentity() {
  std::array<unsigned char,16> b{}; std::random_device rd; for(auto& x:b) x=static_cast<unsigned char>(rd());
  b[6]=static_cast<unsigned char>((b[6]&0x0f)|0x40); b[8]=static_cast<unsigned char>((b[8]&0x3f)|0x80);
  char out[37]{}; std::snprintf(out,sizeof(out),"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7],b[8],b[9],b[10],b[11],b[12],b[13],b[14],b[15]); id_.assign(out);
}
}

