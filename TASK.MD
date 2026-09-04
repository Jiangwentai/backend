在 cpp/src/zmq_publisher.cpp 中，我注意到：

C++
zmq::socket_t socket{context,zmq::socket_type::pub};
socket.set(zmq::sockopt::linger,0);
这里设置了 Linger（退出时丢弃未发送数据，防止阻塞关机），这是很好的。但在高频场景下，ZeroMQ 默认的 SNDHWM（Send High Water Mark，发送高水位标记）通常只有 1000。如果 FastAPI 端（Python）短暂卡顿（比如发生 GC），C++ 端发送超过 1000 条，ZeroMQ 就会在 dontwait 模式下开始丢包，并频繁触发代码中的 spdlog::warn("zeromq_payload_send_would_block")。