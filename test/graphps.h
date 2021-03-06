#ifndef GRAPHPS_H_
#define GRAPHPS_H_

#include "./include/global.h"
#include "./include/dataload.h"
#include <atomic>

class GraphPS {
public:
  GraphPS(){};
//  bool (*_comp)(const int32_t,
//                std::string,
//                const int32_t,
//                T*,
//                T*,
//                const int32_t*,
//                const int32_t*,
//                const int32_t,
//                std::vector<bool>&) = NULL;
  std::string _DataPath;
  VidDtype _VertexTotalNum;
  VidDtype _VertexAllocatedNum;
  VidDtype _VertexDstNum;
  long _EdgeAllocatedNum;
  long _EdgeAllocatedNumAll[9];
  std::atomic<long> _EdgeAllocatedNumAll_Atomic[9];
  int32_t _PartitionTotalNum;
  int32_t _MaxIteration;
  int32_t _PartitionID_Start;
  int32_t _PartitionID_End;
  double _Vertex_Sparse_Ratio;
  VidDtype _VertexID_Start;
  VidDtype _VertexID_End;
  VidDtype _VertexID_Start_Dst;
  VidDtype _VertexID_End_Dst;
  std::vector<int32_t> _Allocated_Partition;
  bool* _VertexWhetherDst[9];
  bool* _VertexWhetherDst_R;
  bool* _VertexWhetherAllocated[9];
  bool* _VertexWhetherAllocated_R;
  std::unordered_map<VidDtype, shared_ptr<VertexData>> _VertexData;
  bloom_parameters _bf_parameters;
  std::map<int32_t, bloom_filter> _bf_pool;
  void init_info_basic(std::string DataPath, const VidDtype VertexNum,
    const int32_t PartitionNum, const int32_t MaxIteration=10);
  void init_info_col();
  void init_info_row();
  void init_bf();
  void build_bf();
  void build_vertex_data(VidDtype* max, VidDtype* min);
  void init_edge_cache();
  void init(std::string DataPath, const VidDtype VertexNum,
    const int32_t PartitionNum, const int32_t MaxIteration=10);
  // virtual void init_vertex()=0;
  void init_vertex(){};
  void run() {};
};

void GraphPS::init_info_basic(std::string DataPath, const VidDtype VertexNum,
  const int32_t PartitionNum, const int32_t MaxIteration) {
  _DataPath = DataPath;
  _VertexTotalNum = VertexNum;
  _PartitionTotalNum = PartitionNum;
  _MaxIteration = MaxIteration;
}

void GraphPS::init_info_col() {
  _my_col = _my_rank%VERTEXCOLNUM;
  _vertex_num_per_col = _VertexTotalNum*1.0/VERTEXCOLNUM;
  for (int i=0; i<VERTEXCOLNUM; i++) {
    Vertex_Col_StartID[i] = std::ceil(i*_vertex_num_per_col);
    Vertex_Col_EndID[i] = std::ceil((i+1)*_vertex_num_per_col);
    Vertex_Col_Len[i] = Vertex_Col_EndID[i] - Vertex_Col_StartID[i];
  }
  _VertexID_Start = Vertex_Col_StartID[_my_col];
  _VertexID_End = Vertex_Col_EndID[_my_col];
  for (int i=0; i<_num_workers; i++) {
    _col_to_ranks[i%VERTEXCOLNUM].push_back(i);
  }
  if (_my_rank == 0) {
    LOG(INFO) << "Vertex Col Information";
    for (int i=0; i<VERTEXCOLNUM; i++) {
      LOG(INFO) << "Col " << i << ": "
        << "From " << Vertex_Col_StartID[i] << ", " 
        << "To " << Vertex_Col_EndID[i];
    }
  }
  barrier_workers();
}

void GraphPS::init_info_row() {
  _my_row = int(_my_rank/VERTEXCOLNUM);
  for (int i=0; i<VERTEXCOLNUM; i++) {
    int32_t row_in_process = 0;
    unsigned long total_size_col = 0;
    double avg_size_row = 0;
    unsigned long alloc_size = 0;
    for (int j=0; j<_PartitionTotalNum; j++) {
      std::string DataPath;
      DataPath = _DataPath + std::to_string(j);
      DataPath += "-";
      DataPath += std::to_string(i);
      DataPath += ".edge.npy";
      total_size_col += get_file_size(DataPath.c_str());
    }
    avg_size_row = total_size_col*1.0/VERTEXROWNUM;
    for (int j=0; j<_PartitionTotalNum; j++) {
      std::string DataPath;
      DataPath = _DataPath + std::to_string(j);
      DataPath += "-";
      DataPath += std::to_string(i);
      DataPath += ".edge.npy";
      alloc_size += get_file_size(DataPath.c_str());
      if (alloc_size >= avg_size_row) {
        if (_my_row == row_in_process && _my_col == i) {
          _PartitionID_End = j;
        }
        if (_my_row == row_in_process+1 && _my_col == i) {
          _PartitionID_Start = j;
        }
        alloc_size = 0;
        row_in_process++;
      }
      if (row_in_process == VERTEXROWNUM) {break;}
    }
  }
  if (_my_row == 0) {_PartitionID_Start = 0;}
  if (_my_row == VERTEXROWNUM-1) {_PartitionID_End = _PartitionTotalNum;}
  for (int i = _PartitionID_Start; i < _PartitionID_End; i++) {
      _Allocated_Partition.push_back(i);
  }
  LOG(INFO) << "Rank: " << _my_rank << ", " 
    << "Row: " << _my_row << ", " 
    << "Col: " << _my_col << ", " 
    << "Partition From " << _PartitionID_Start << ", to " << _PartitionID_End;
  barrier_workers();
}

void GraphPS::init_edge_cache() {
  _EdgeCache.reserve(_PartitionID_End - _PartitionID_Start);
  for (int i = 0; i < CMPNUM; i++) {
    _Edge_Buffer[i] = NULL;
    _Edge_Buffer_Lock[i] = 0;
    _Edge_Buffer_Len[i] = 0;
    _Uncompressed_Buffer[i] = NULL;
    _Uncompressed_Buffer_Lock[i] = 0;
    _Uncompressed_Buffer_Len[i] = 0;
  }
  int32_t data_size = GetDataSize(_DataPath) * 1.0 / 1024 / 1024 / 1024; //GB 
  int32_t cache_size = _num_workers * EDGE_CACHE_SIZE / 1024; //GB
  if (data_size <= cache_size*1.1) 
    COMPRESS_CACHE_LEVEL = 0;
  else if (data_size * 0.5 <= cache_size*1.1)
    COMPRESS_CACHE_LEVEL = 1;
  else if (data_size * 0.25 <= cache_size*1.1)
    COMPRESS_CACHE_LEVEL = 2;
  else
    COMPRESS_CACHE_LEVEL = 3;
  LOG(INFO) << "data size "  << data_size << " GB, "
            << "cache size " << cache_size << " GB, "
            << "compress level " << COMPRESS_CACHE_LEVEL;
}

void GraphPS::build_bf() {
  #pragma omp parallel for num_threads(CMPNUM) schedule(static)
  for (int32_t t_pid = _PartitionID_Start; t_pid < _PartitionID_End; t_pid++) {
    std::string DataPath;
    DataPath = _DataPath + std::to_string(t_pid);
    DataPath += "-";
    DataPath += std::to_string(_my_col);
    DataPath += ".edge.npy";
    // LOG(INFO) << DataPath;
    char* EdgeDataNpy = load_edge(t_pid, DataPath);
    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t v_num = EdgeData[0];
    int32_t *p = EdgeData+1;
    int32_t v=0;
    int32_t l=0;
    for (int i=0; i < v_num; i++) {
      p++; v = *p;
      
      /*
      size_t sid_hash = std::hash<std::string>{}(std::to_string(v));
      int node_id = sid_hash%_num_workers;
      _VertexWhetherDst[node_id][v] = true;
      */
     
     /*
      int node_id = _my_rank;
       _VertexWhetherDst[node_id][v] = true;
     */

      p++; l = *p;
      for (int k = 0; k < l; k++) {
        p++;
       
       /*
        size_t sid_hash = std::hash<std::string>{}(std::to_string(v)+"-"+std::to_string(*p));
        int node_id = sid_hash%_num_workers;
        _VertexWhetherDst[node_id][v] = true;
        */

      
        size_t sid_hash = std::hash<std::string>{}(std::to_string(*p));
        int node_id = sid_hash%_num_workers;
        _VertexWhetherDst[node_id][v] = true;
       

        // _bf_pool[t_pid].insert(indices[indptr[i] + k]);
        _EdgeAllocatedNumAll_Atomic[node_id]++;
        _VertexWhetherAllocated[node_id][*p] = true;
      }
    }
  }
  bool* _VWAT = new bool[_VertexTotalNum];
  bool* _VWAT2 = new bool[_VertexTotalNum];
  for (int i=0; i<_num_workers; i++) {
    _EdgeAllocatedNumAll[i] = _EdgeAllocatedNumAll_Atomic[i];
    memset(_VWAT, 0, _VertexTotalNum);
    memset(_VWAT2, 0, _VertexTotalNum);
    void* p_send = _VertexWhetherAllocated[i];
    void* p_recv = _VWAT;
    void* p_send2 = _VertexWhetherDst[i];
    void* p_recv2 = _VWAT2;
    MPI_Reduce(p_send, p_recv, _VertexTotalNum, MPI_C_BOOL, MPI_LOR, i, MPI_COMM_WORLD);
    MPI_Reduce(p_send2, p_recv2, _VertexTotalNum, MPI_C_BOOL, MPI_LOR, i, MPI_COMM_WORLD);
    MPI_Reduce(&_EdgeAllocatedNumAll[i], &_EdgeAllocatedNum, 1, MPI_LONG, MPI_SUM, i, MPI_COMM_WORLD);
    if (_my_rank == i) {
      for (int j=0; j<_VertexTotalNum; j++) {
        _VertexWhetherAllocated_R[j] = _VWAT[j];
        _VertexWhetherDst_R[j] = _VWAT2[j];
      }
    }
  }
}

void GraphPS::build_vertex_data(VidDtype *min, VidDtype *max) {
  for (VidDtype i=0; i < _VertexTotalNum; i++) {
    if (_VertexWhetherAllocated_R[i] == true) {
      // _VertexData[i] = boost::make_shared<VertexData>();
      if (i<*min) {*min=i;}
      if (i>*max) {*max=i;}
    }
  }
}

void GraphPS::init_bf() {
  _EdgeAllocatedNum = 0;
  for (int i=0; i<9; i++) {
    _EdgeAllocatedNumAll[i] = 0;
    _EdgeAllocatedNumAll_Atomic[i] = 0;
  }
  _VertexAllocatedNum = 0;
  _VertexWhetherAllocated_R = new bool[_VertexTotalNum];
  memset(_VertexWhetherAllocated_R, 0, _VertexTotalNum);
  _VertexWhetherDst_R = new bool[_VertexTotalNum];
  memset(_VertexWhetherDst_R, 0, _VertexTotalNum);
  for (int i=0; i<9; i++) {
    _VertexWhetherAllocated[i] = new bool[_VertexTotalNum];
    memset(_VertexWhetherAllocated[i], 0, _VertexTotalNum);
    _VertexWhetherDst[i] = new bool[_VertexTotalNum];
    memset(_VertexWhetherDst[i], 0, _VertexTotalNum);
  }
  _bf_parameters.projected_element_count = 1000000;
  _bf_parameters.false_positive_probability = 0.01;
  _bf_parameters.random_seed = 0xA5A5A5A5;
  if (!_bf_parameters) {assert(1==0);}
  _bf_parameters.compute_optimal_parameters();
  for (int32_t k=_PartitionID_Start; k<_PartitionID_End; k++) {
    _bf_pool[k] = bloom_filter(_bf_parameters);
  }
  build_bf();
  VidDtype real_start_vid = 0;
  VidDtype real_end_vid = 0;

  build_vertex_data(&real_start_vid, &real_end_vid);
  VidDtype sum_of_elems = 0;
  for (int n=0; n<_VertexTotalNum; n++) {
     sum_of_elems += _VertexWhetherAllocated_R[n];
  }
  // assert(sum_of_elems == VidDtype(_VertexData.size()));
  _VertexAllocatedNum = sum_of_elems;
  _Vertex_Sparse_Ratio = _VertexAllocatedNum*1.0/(_VertexTotalNum);

  sum_of_elems = 0;
  for (int n=0; n<_VertexTotalNum; n++) {
     sum_of_elems += _VertexWhetherDst_R[n];
  }
  _VertexDstNum = sum_of_elems;

  LOG(INFO) << "Rank " << _my_rank << " Manages " << _VertexAllocatedNum 
    << " V "  << _VertexAllocatedNum*1.0/_VertexTotalNum
    << " " << _EdgeAllocatedNum << " E"
    << " Updates " << _VertexDstNum*1.0/_VertexTotalNum << " V";
  barrier_workers();
  LOG(INFO) << "Rank " << _my_rank << " Read Vertex From " << real_start_vid << "/" << _VertexID_Start
    << " To " << real_end_vid << "/" << _VertexID_End;
  double sparse_ratio_total = 0;
  MPI_Allreduce(&_Vertex_Sparse_Ratio, &sparse_ratio_total, 1, MPI_DOUBLE, MPI_SUM,  MPI_COMM_WORLD);
  _Vertex_Sparse_Ratio = sparse_ratio_total/_num_workers;
}

void GraphPS::init(std::string DataPath, const VidDtype VertexNum, const int32_t PartitionNum, const int32_t MaxIteration) {
  assert(_num_workers == VERTEXCOLNUM * VERTEXROWNUM);
  start_time_init();
  init_info_basic(DataPath, VertexNum, PartitionNum, MaxIteration);
  init_info_col();
  init_info_row();
  init_edge_cache();
  init_bf();
}


#endif
