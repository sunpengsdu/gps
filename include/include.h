#ifndef SYSTEM_INCLUDE_H_
#define SYSTEM_INCLUDE_H_

#include "../thrift/gen-cpp/VertexUpdate.h"
#include "cnpy.h"
#include "bloom_filter.hpp"
#include <thrift/transport/TServerSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TZlibTransport.h>
#include <thread>
#include <map>
#include <chrono>
#include <array>
#include <unordered_map>
#include <tuple>
#include <chrono>
#include <future>
#include <exception>
#include <atomic>
#include <algorithm>
#include <vector>
#include <iostream>
#include <cmath>
#include <stdlib.h>
#include <omp.h>
#include <sched.h>
#include <string.h>
#include <sys/stat.h>
#include <snappy.h>
#include <zlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <glog/logging.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;
using namespace  ::graphps;

#define SERVER_PORT 9090
#define HOST_LEN 20
#define COMPRESS
#define GPS_INF 10000
#define EDGE_CACHE_SIZE 50*1024 //MB
#define DENSITY_VALUE 20
#define BF_THRE 0.001
#define CMPNUM 23
#define COMNUM 1
#define VERTEXCOLNUM 9
#define VERTEXROWNUM 1
#define COMPRESS_COMMU_LEVEL 1
#define COMPRESS_CACHE_LEVEL 1

double vertex_num_per_col;
std::array<int32_t, VERTEXCOLNUM> Vertex_Col_StartID;
std::array<int32_t, VERTEXCOLNUM> Vertex_Col_EndID;
std::array<int32_t, VERTEXCOLNUM> Vertex_Col_Len;
std::array<std::vector<int>, VERTEXCOLNUM> _col_to_ranks;

std::chrono::steady_clock::time_point INIT_TIME_START;
std::chrono::steady_clock::time_point INIT_TIME_END;
std::chrono::steady_clock::time_point COMP_TIME_START;
std::chrono::steady_clock::time_point COMP_TIME_END;
std::chrono::steady_clock::time_point APP_TIME_START;
std::chrono::steady_clock::time_point APP_TIME_END;

int64_t INIT_TIME;
int64_t COMP_TIME;
int64_t APP_TIME;
struct EdgeCacheData {
  char * data;
  int32_t compressed_length;
  int32_t uncompressed_length;
};

std::unordered_map<int32_t, EdgeCacheData> _EdgeCache;
std::atomic<int32_t> _EdgeCache_Size;
std::atomic<int32_t> _EdgeCache_Size_Uncompress;
std::atomic<int32_t> _Computing_Num;
std::atomic<int32_t> _Missed_Num;
std::atomic<long> _Network_Compressed;
std::atomic<long> _Network_Uncompressed;
std::atomic<long> _Changed_Vertex;

std::unordered_map<int, char*> _Edge_Buffer;
std::unordered_map<int, size_t> _Edge_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Edge_Buffer_Lock;
std::unordered_map<int, char*> _Uncompressed_Buffer;
std::unordered_map<int, size_t> _Uncompressed_Buffer_Len;
std::unordered_map<int, std::atomic<int>> _Uncompressed_Buffer_Lock;
std::unordered_map<int, std::unordered_map<int, void*>> _Socket_Pool;

int _server_port = SERVER_PORT;
int16_t _comp_level = COMPRESS_COMMU_LEVEL;
int _my_rank;
int _my_col;
int _num_workers;
int _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;
std::map<int, std::string> _map_hosts;

int DEFAULT_URBUF_SIZE_SP = 2048;
int DEFAULT_CRBUF_SIZE_SP = 2048;
int DEFAULT_UWBUF_SIZE_SP = 2048;
int DEFAULT_CWBUF_SIZE_SP = 2048;

#endif
