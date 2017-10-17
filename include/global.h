#include "../thrift/gen-cpp/VertexUpdate.h"
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
#include <mpi.h>
#include <glog/logging.h>

#define HOST_LEN 20
#define COMPRESS

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;
using namespace  ::graphps;

int _server_port = 9090;
int _server_threadnum = 1;
int16_t _comp_level = 1;
int _my_rank;
int _num_workers;
int _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;
std::map<int, std::string> _map_hosts;

int DEFAULT_URBUF_SIZE_SP = 2048;
int DEFAULT_CRBUF_SIZE_SP = 2048;
int DEFAULT_UWBUF_SIZE_SP = 2048;
int DEFAULT_CWBUF_SIZE_SP = 2048;

class TZlibBufferdTransportFactory : public TTransportFactory {
 public:
  TZlibBufferdTransportFactory() {}
  virtual ~TZlibBufferdTransportFactory() {}
  virtual shared_ptr<TTransport> getTransport(shared_ptr<TTransport> trans) {
    shared_ptr<TTransport> bufferdtransport(new TBufferedTransport(trans));
    return shared_ptr<TTransport>(new TZlibTransport(bufferdtransport, 
      DEFAULT_URBUF_SIZE_SP, DEFAULT_CRBUF_SIZE_SP,
      DEFAULT_UWBUF_SIZE_SP, DEFAULT_CWBUF_SIZE_SP, _comp_level));
  }
};

void sleep_ms(int x) {
  std::this_thread::sleep_for(std::chrono::milliseconds(x));
}

void init_nodes() {
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
  MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
  MPI_Get_processor_name(_hostname, &_hostname_len);
  _all_hostname = new char[HOST_LEN * _num_workers];
  memset(_all_hostname, 0, HOST_LEN * _num_workers);
  MPI_Allgather(_hostname, HOST_LEN, MPI_CHAR, _all_hostname, HOST_LEN, MPI_CHAR, MPI_COMM_WORLD);
  if (_my_rank == 0) {
    LOG(INFO) << "Processors: " << _num_workers;
    for (int i = 0; i < _num_workers; i++) {
      LOG(INFO) << "Rank " << i << ": " << _all_hostname + HOST_LEN *i;
    }
  }
  for (int i = 0; i < _num_workers; i++) {
    std::string host_name(_all_hostname + i * HOST_LEN);
    _map_hosts[i] = host_name;
  }
  MPI_Barrier(MPI_COMM_WORLD);
}
