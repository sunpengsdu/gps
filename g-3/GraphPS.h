/*
 *Copyright 2015 NTU (http://www.ntu.edu.sg/)
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
*/
#ifndef GRAPHPS_H_
#define GRAPHPS_H_

#include "Global.h"
#include "Communication.h"
#include "Comp.h"

template<class T>
class GraphPS {
public:
  bool (*_comp)(const int32_t,
                std::string,
                const int32_t,
                T*,
                T*,
                const int32_t*,
                const int32_t*,
                const int32_t,
                std::vector<bool>&) = NULL;
  T _FilterThreshold;
  std::string _DataPath;
  std::string _Scheduler;
  int32_t _ThreadNum;
  int32_t _VertexNum;
  int32_t _PartitionNum;
  int32_t _MaxIteration;
  int32_t _PartitionID_Start;
  int32_t _PartitionID_End;
  std::vector<bool> _Vertex_State;
  std::vector<int32_t> _Allocated_Partition;
  std::map<int, std::string> _AllHosts;
  std::vector<int32_t> _VertexOut;
  std::vector<int32_t> _VertexIn;
  std::vector<T> _VertexMsg;
  std::vector<T> _VertexMsgNew;
  bloom_parameters _bf_parameters;
  std::map<int32_t, bloom_filter> _bf_pool;
  GraphPS();
  void init(std::string DataPath,
            const int32_t VertexNum,
            const int32_t PartitionNum,
            const int32_t MaxIteration=10);

//    virtual void compute(const int32_t PartitionID)=0;
  virtual void init_vertex()=0;
  void set_threadnum (const int32_t ThreadNum);
  void run();
  void load_vertex_in();
  void load_vertex_out();
};

template<class T>
GraphPS<T>::GraphPS() {
  _VertexNum = 0;
  _PartitionNum = 0;
  _MaxIteration = 0;
  _ThreadNum = 1;
  _PartitionID_Start = 0;
  _PartitionID_End = 0;
}

template<class T>
void GraphPS<T>::init(std::string DataPath,
                      const int32_t VertexNum,
                      const int32_t PartitionNum,
                      const int32_t MaxIteration) {
  start_time_init();
  _ThreadNum = CMPNUM;
  _DataPath = DataPath;
  _VertexNum = VertexNum;
  _PartitionNum = PartitionNum;
  _MaxIteration = MaxIteration;
  for (int i = 0; i < _num_workers; i++) {
    std::string host_name(_all_hostname + i * HOST_LEN);
    _AllHosts[i] = host_name;
  }
  _Scheduler = _AllHosts[0];
  int32_t n = std::ceil(_PartitionNum*1.0/VERTEXROWNUM);
  int32_t my_row = int(_my_rank/VERTEXCOLNUM);
  assert(_num_workers == 9);

  // _PartitionID_Start = (int(_my_rank/VERTEXCOLNUM)*n < _PartitionNum) ? int(_my_rank/VERTEXCOLNUM)*n:-1;
  // _PartitionID_End = ((1+int(_my_rank/VERTEXCOLNUM))*n > _PartitionNum) ? _PartitionNum:(1+int(_my_rank/VERTEXCOLNUM))*n;

  // _PartitionID_Start = 0;
  // _PartitionID_End = _PartitionNum;

  vertex_num_per_col = _VertexNum*1.0/VERTEXCOLNUM;
  for (int i=0; i<VERTEXCOLNUM; i++){
    Vertex_Col_StartID[i] = std::ceil(i*vertex_num_per_col);
    Vertex_Col_EndID[i] = std::ceil((i+1)*vertex_num_per_col);
    Vertex_Col_Len[i] = Vertex_Col_EndID[i] - Vertex_Col_StartID[i];
  }
  _my_col = _my_rank%VERTEXCOLNUM;
  for (int i=0; i<_num_workers; i++) {
    _col_to_ranks[i%VERTEXCOLNUM].push_back(i);
  }
  if (_my_rank == 0) {
    std::cout << "Vertex Col Information\n";
    for (int i=0; i<VERTEXCOLNUM; i++) {
      std::cout << Vertex_Col_StartID[i] << " ," << Vertex_Col_EndID[i] << " ," << Vertex_Col_Len[i] << "\n";
    }
  }
  _Vertex_State.assign(Vertex_Col_Len[_my_col], true);

  for (int i=0; i<VERTEXCOLNUM; i++) {
    int row_in_process = 0;
    unsigned long total_size_col = 0;
    double avg_size_row = 0;
    unsigned long alloc_size = 0;
    for (int j=0; j<_PartitionNum; j++) {
      std::string DataPath;
      DataPath = _DataPath + std::to_string(j);
      DataPath += "-";
      DataPath += std::to_string(i);
      DataPath += ".edge.npy";
      total_size_col += get_file_size(DataPath.c_str());
    }
    avg_size_row = total_size_col*1.0/3;
    // if (_my_rank==0) {
    //   std::cout << avg_size_row << "\n";
    // }
    for (int j=0; j<_PartitionNum; j++) {
      std::string DataPath;
      DataPath = _DataPath + std::to_string(j);
      DataPath += "-";
      DataPath += std::to_string(i);
      DataPath += ".edge.npy";
      alloc_size += get_file_size(DataPath.c_str());
      if (alloc_size >= avg_size_row) {
        if (my_row == row_in_process && _my_col == i) {
          _PartitionID_End = j;
        }
        if (my_row == row_in_process+1 && _my_col == i) {
          _PartitionID_Start = j;
        }
        alloc_size = 0;
        row_in_process++;
      }
      if (row_in_process == 2) {break;}
    }
  }
  if (my_row == 0) {
    _PartitionID_Start = 0;
  }
  if (my_row == 2) {
    _PartitionID_End = _PartitionNum;
  }

  // ############# BLOOMFILTER
#ifdef USE_BF
  _bf_parameters.projected_element_count = 1000000;
  _bf_parameters.false_positive_probability = 0.01;
  _bf_parameters.random_seed = 0xA5A5A5A5;
  if (!_bf_parameters) {assert(1==0);}
  _bf_parameters.compute_optimal_parameters();
  for (int32_t k=_PartitionID_Start; k<_PartitionID_End; k++) {
    _bf_pool[k] = bloom_filter(_bf_parameters);
  }
#endif
  for (int i = _PartitionID_Start; i < _PartitionID_End; i++) {
    _Allocated_Partition.push_back(i);
  }

  LOG(INFO) << "Rank " << _my_rank << " "
            << " With Partitions From " << _PartitionID_Start << " To " << _PartitionID_End << " Col " << _my_col;

  _EdgeCache.reserve(_PartitionNum*2/_num_workers);
  for (int i = 0; i < _ThreadNum; i++) {
    _Send_Buffer[i] = NULL;
    _Send_Buffer_Lock[i] = 0;
    _Send_Buffer_Len[i] = 0;
    _Edge_Buffer[i] = NULL;
    _Edge_Buffer_Lock[i] = 0;
    _Edge_Buffer_Len[i] = 0;
    _Uncompressed_Buffer[i] = NULL;
    _Uncompressed_Buffer_Lock[i] = 0;
    _Uncompressed_Buffer_Len[i] = 0;
  }
  int32_t data_size = GetDataSize(DataPath) * 1.0 / 1024 / 1024 / 1024; //GB 
  int32_t cache_size = _num_workers * EDGE_CACHE_SIZE / 1024; //GB
  //0:1, 1:0.5, 2:0.25, 3:0.2
  if (data_size <= cache_size*1.1) 
    COMPRESS_CACHE_LEVEL = 0;
  else if (data_size * 0.5 <= cache_size*1.1)
    COMPRESS_CACHE_LEVEL = 1;
  else if (data_size * 0.25 <= cache_size*1.1)
    COMPRESS_CACHE_LEVEL = 2;
  else
    COMPRESS_CACHE_LEVEL = 3;
  //#########################
  // COMPRESS_CACHE_LEVEL = 0;
  LOG(INFO) << "data size "  << data_size << " GB, "
            << "cache size " << cache_size << " GB, "
            << "compress level " << COMPRESS_CACHE_LEVEL;
}

template<class T>
void  GraphPS<T>::load_vertex_in() {
  std::string vin_path = _DataPath + "indegree.npy";
  cnpy::NpyArray npz = cnpy::npy_load(vin_path);
  int32_t *data = reinterpret_cast<int32_t*>(npz.data);
  _VertexIn.assign(data+Vertex_Col_StartID[_my_col], data+Vertex_Col_EndID[_my_col]);
  npz.destruct();
}

template<class T>
void  GraphPS<T>::load_vertex_out() {
  std::string vout_path = _DataPath + "outdegree.npy";
  cnpy::NpyArray npz = cnpy::npy_load(vout_path);
  int32_t *data = reinterpret_cast<int32_t*>(npz.data);
  _VertexOut.assign(data+Vertex_Col_StartID[_my_col], data+Vertex_Col_EndID[_my_col]);
  npz.destruct();
}

template<class T>
void GraphPS<T>::run() {
  /////////////////
  #ifdef USE_HDFS
  LOG(INFO) << "Rank " << _my_rank << " Loading Edge From HDFS";
  start_time_hdfs();
  int hdfs_re = 0;
  hdfs_re = system("rm /home/mapred/tmp/satgraph/*");
  std::string hdfs_bin = "/opt/hadoop-1.2.1/bin/hadoop fs -get ";
  std::string hdfs_dst = "/home/mapred/tmp/satgraph/";
  #pragma omp parallel for num_threads(6) schedule(static)
  for (int32_t k=0; k<_Allocated_Partition.size(); k++) {
    std::string hdfs_command;
    hdfs_command = hdfs_bin + _DataPath;
    hdfs_command += std::to_string(_Allocated_Partition[k]);
    hdfs_command += ".edge.npy ";
    hdfs_command += hdfs_dst;
    hdfs_re = system(hdfs_command.c_str());
  }

  LOG(INFO) << "Rank " << _my_rank << " Loading Vertex From HDFS";
  std::string hdfs_command;
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexin.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  hdfs_command.clear();
  hdfs_command = hdfs_bin + _DataPath;
  hdfs_command += "vertexout.npy ";
  hdfs_command += hdfs_dst;
  hdfs_re = system(hdfs_command.c_str());
  stop_time_hdfs();
  barrier_workers();
  if (_my_rank==0)
    LOG(INFO) << "HDFS  Load Time: " << HDFS_TIME << " ms";
  _DataPath.clear();
  _DataPath = hdfs_dst;
  #endif
  ////////////////

  init_vertex();

  std::vector<std::mutex> lock_partitions(_PartitionNum);
  std::thread graphps_server_mt(graphps_server<T>, std::ref(_VertexMsgNew), std::ref(_VertexMsg), std::ref(lock_partitions));
  // std::vector<int32_t> ActiveVector_V;
  std::vector<int32_t> Partitions(_Allocated_Partition.size(), 0);
  float updated_ratio = 1.0;
  int32_t step = 0;

  std::vector<int32_t> VertexActive;

#ifdef USE_BF
// std::vector<bool> VertexStatus_Bool(_VertexNum, true);
  std::vector<bool> PartitionActive_Bool(_PartitionID_End - _PartitionID_Start, true);
  #pragma omp parallel for num_threads(_ThreadNum) schedule(static)
  for (int32_t t_pid = _PartitionID_Start; t_pid < _PartitionID_End; t_pid++) {
    std::string DataPath;
    DataPath = _DataPath + std::to_string(t_pid);
    DataPath += "-";
    DataPath += std::to_string(_my_col);
    DataPath += ".edge.npy";
    char* EdgeDataNpy = load_edge(t_pid, DataPath);
    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t t_start_id = EdgeData[3];
    int32_t t_end_id = EdgeData[4];
    int32_t indices_len = EdgeData[1];
    int32_t indptr_len = EdgeData[2];
    int32_t * indices = EdgeData + 5;
    int32_t * indptr = EdgeData + 5 + indices_len;
    int32_t i   = 0;
    int32_t k   = 0;
    for (i=0; i < t_end_id - t_start_id; i++) {
      for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
        _bf_pool[t_pid].insert(indices[indptr[i] + k]);
      }
    }
  }
#endif
  
 // _VertexMsgNew.assign(_VertexMsg.begin(), _VertexMsg.end());

  barrier_workers();
  stop_time_init();
  if (_my_rank==0)
    LOG(INFO) << "Init Time: " << INIT_TIME << " ms";
  LOG(INFO) << "Rank " << _my_rank << " use " << _ThreadNum << " comp threads";

  // start computation
  for (step = 0; step < _MaxIteration; step++) {
    start_time_comp();
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    updated_ratio = 1.0;
    for (int32_t k = 0; k < _Allocated_Partition.size(); k++) {
      Partitions[k] = _Allocated_Partition[k];
    }
    std::random_shuffle(Partitions.begin(), Partitions.end());

    // while(1);

    #pragma omp parallel for num_threads(_ThreadNum) schedule(dynamic)
    for (int32_t k=0; k<Partitions.size(); k++) {
      int32_t P_ID = Partitions[k];
#ifdef USE_BF
      if (PartitionActive_Bool[P_ID - _PartitionID_Start] == false)
        continue;
#endif

      (*_comp)(P_ID,  _DataPath, _VertexNum,
               _VertexMsg.data(), _VertexMsgNew.data(),
               _VertexOut.data(), _VertexIn.data(),
               step, std::ref(_Vertex_State));
    }
    while(_Pending_Request > 0) {
      graphps_sleep(10);
    }
    std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
    int local_comp_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count();
    LOG(INFO) << "Iter: " << step << " Worker: " << _my_rank << " Use: " << local_comp_time;

    barrier_workers();

    long changed_num = 0;
    #pragma omp parallel for num_threads(_ThreadNum)  schedule(static) reduction(+:changed_num)
    for (int32_t result_id = 0; result_id < Vertex_Col_Len[_my_col]; result_id++) {
      if (_VertexMsg[result_id] != _VertexMsgNew[result_id]) {
        changed_num++;
        _Vertex_State[result_id] = true;
      } else {
        _Vertex_State[result_id] = false;
      }
    }
    long changed_num_total = 0;
    MPI_Allreduce(&changed_num, &changed_num_total, 1, MPI_LONG, MPI_SUM,  MPI_COMM_WORLD);
    changed_num_total = changed_num_total / 3;
   // MPI_Bcast(&changed_num_total, 1, MPI_INT, 0,  MPI_COMM_WORLD);

    updated_ratio = changed_num_total * 1.0 / _VertexNum;
    int missed_num = _Missed_Num;
    int total_missed_num = _Missed_Num;
    int cache_size = _EdgeCache_Size;
    int total_cache_size = _EdgeCache_Size;
    int cache_size_uncompress = _EdgeCache_Size_Uncompress;
    int total_cache_size_uncompress = _EdgeCache_Size_Uncompress;
    long network_compress = _Network_Compressed;
    long network_uncompress = _Network_Uncompressed;
    long total_network_compress = _Network_Compressed;
    long total_network_uncompress = _Network_Uncompressed;
    ///*
    MPI_Reduce(&missed_num, &total_missed_num, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&cache_size, &total_cache_size, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&cache_size_uncompress, &total_cache_size_uncompress, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&network_compress, &total_network_compress, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&network_uncompress, &total_network_uncompress, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    //*/
    VertexActive.clear();
    
    float updated_ratio_t = updated_ratio;
    #ifdef PAGERANK
    updated_ratio_t = 1;
    #endif
    if (updated_ratio_t < BF_THRE) {
      omp_lock_t lock;
      omp_init_lock(&lock);
      #pragma omp parallel for num_threads(_ThreadNum) schedule(static)
      for (int32_t result_id = 0; result_id < Vertex_Col_Len[_my_col]; result_id++) {
      // ############## BLOOMFILTER
#ifdef USE_BF
      if (_VertexMsgNew[result_id] != _VertexMsg[result_id]) {
        omp_set_lock(&lock);
        VertexActive.push_back(result_id);
        omp_unset_lock(&lock);
      }
#endif
        _VertexMsg[result_id] = _VertexMsgNew[result_id];
      } 
    } else {
      #pragma omp parallel for num_threads(_ThreadNum)  schedule(static)
      for (int32_t result_id = 0; result_id < Vertex_Col_Len[_my_col]; result_id++) {
        _VertexMsg[result_id] = _VertexMsgNew[result_id];
        #ifdef PAGERANK
        _VertexMsgNew[result_id] = 0;
        #endif
      }
    }

#ifdef USE_BF
    // if (changed_num_total < 1000) {
    if (updated_ratio_t < BF_THRE) {
      #pragma omp parallel for num_threads(_ThreadNum)  schedule(static)
      for (int32_t result_id = 0; result_id < _PartitionID_End - _PartitionID_Start; result_id++) {
        PartitionActive_Bool[result_id] = false;
        for (int32_t t_vindex=0; t_vindex < VertexActive.size(); t_vindex++) {
          if (_bf_pool[result_id+_PartitionID_Start].contains(VertexActive[t_vindex])) {
            PartitionActive_Bool[result_id] = true;
            break;
          }
        }
      }
    } else {
      #pragma omp parallel for num_threads(_ThreadNum)  schedule(static)
      for (int32_t result_id = 0; result_id < _PartitionID_End - _PartitionID_Start; result_id++) {
        PartitionActive_Bool[result_id] = true;
      }
    }
#endif

    stop_time_comp();
    _Missed_Num = 0;
    _Network_Compressed = 0;
    _Network_Uncompressed = 0;
    if (_my_rank==0)
      LOG(INFO) << "Iteration: " << step
                << ", uses "<< COMP_TIME
                << " ms, Update " << changed_num_total
                << ", Ratio " << updated_ratio
                << ", Miss " << total_missed_num
                << ", Cache(MB) " << total_cache_size
                << ", Before(MB) " << total_cache_size_uncompress
                << ", Compress Net(MB) " << total_network_compress*1.0/1024/1024
                << ", Uncompress Net(MB) " << total_network_uncompress*1.0/1024/1024;
    if (changed_num_total == 0 && step > 1) {
      break;
    }
  }
}

#endif /* GRAPHPS_H_ */
