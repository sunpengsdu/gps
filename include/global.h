#include "../thrift/gen-cpp/VertexUpdate.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TThreadPoolServer.h>    
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadedServer.h> 
#include <thrift/concurrency/ThreadManager.h>    
#include <thrift/concurrency/PosixThreadFactory.h>  
#include <thread>
#include <mpi.h>
#include <glog/logging.h>

#define HOST_LEN 20

int _server_port = 9090;
int _server_threadnum = 1;
int _my_rank;
int _num_workers;
int _hostname_len;
char _hostname[HOST_LEN];
char *_all_hostname;

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
  MPI_Barrier(MPI_COMM_WORLD);
}
