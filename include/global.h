#ifndef INCLUDE_GLOBAL_H_
#define INCLUDE_GLOBAL_H_

#include "include.h"
#include "dataload.h"

// int32_t get_col_id(int32_t vertex_id) {
//   return std::floor(vertex_id/_vertex_num_per_col);
// }

void barrier_workers() {
  MPI_Barrier(MPI_COMM_WORLD);
}

void finalize_workers() {
  LOG(INFO) << "Finalizing the application";
  delete [] (_all_hostname);
  for (auto t_it = _EdgeCache.begin(); t_it != _EdgeCache.end(); t_it++) {
    delete [] t_it->second.data;
  }
  for (int32_t i = 0; i < CMPNUM; i++) {
    delete [] (_Edge_Buffer[i]);
    delete [] (_Uncompressed_Buffer[i]);
  }
  MPI_Finalize();
}

void start_time_app() {
  APP_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_app() {
  APP_TIME_END = std::chrono::steady_clock::now();
  APP_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
             (APP_TIME_END-APP_TIME_START).count();
}

void start_time_init() {
  INIT_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_init() {
  INIT_TIME_END = std::chrono::steady_clock::now();
  INIT_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
              (INIT_TIME_END-INIT_TIME_START).count();
}

void start_time_comp() {
  COMP_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_comp() {
  COMP_TIME_END = std::chrono::steady_clock::now();
  COMP_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
              (COMP_TIME_END-COMP_TIME_START).count();
}

void start_time_iter() {
  ITER_TIME_START = std::chrono::steady_clock::now();
}

void stop_time_iter() {
  ITER_TIME_END = std::chrono::steady_clock::now();
  ITER_TIME = std::chrono::duration_cast<std::chrono::milliseconds>
              (ITER_TIME_END-ITER_TIME_START).count();
}



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
  _EdgeCache_Size = 0;
  _EdgeCache_Size_Uncompress = 0;
  _Computing_Num = 0;
  _Missed_Num = 0;
  _Network_Compressed = 0;
  _Network_Uncompressed = 0;
  _Changed_Vertex = 0;
  MPI_Barrier(MPI_COMM_WORLD);
}

#endif
