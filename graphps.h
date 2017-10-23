#ifndef GRAPHPS_H_
#define GRAPHPS_H_

#include "./include/global.h"
#include "./include/dataload.h"

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
  VidDtype _VertexDestNum;
  int64_t _EdgeAllocatedNum;
  int32_t _PartitionTotalNum;
  int32_t _MaxIteration;
  int32_t _PartitionID_Start;
  int32_t _PartitionID_End;
  double _Vertex_Need_Ratio;
  std::vector<std::bitset<VERTEXROWNUM*VERTEXCOLNUM>> _VertexToNodes;
  std::vector<int32_t> _Allocated_Partition;
  std::vector<int32_t> _All_Partition;
  bool* _VertexWhetherAllocated;
  bool* _VertexWhetherDest;
  std::map<VidDtype, VertexData*> _VertexData;
  bloom_parameters _bf_parameters;
  std::map<int32_t, bloom_filter> _bf_pool;
  void init_info_basic(std::string DataPath, const VidDtype VertexNum,
    const int32_t PartitionNum, const int32_t MaxIteration=10);
  void init_info_col();
  void init_info_row();
  void init_bf();
  void build_bf();
  void build_vertex_data();
  void build_vertex_node();
  void init_edge_cache();
  void init(std::string DataPath, const VidDtype VertexNum,
    const int32_t PartitionNum, const int32_t MaxIteration=10);
  void load_vertex_outdegree();
  // virtual void init_vertex_data()=0;
  void init_vertex_data();
  void run();
};

void GraphPS::init_info_basic(std::string DataPath, const VidDtype VertexNum,
  const int32_t PartitionNum, const int32_t MaxIteration) {
  _DataPath = DataPath;
  _VertexTotalNum = VertexNum;
  _PartitionTotalNum = PartitionNum;
  _MaxIteration = MaxIteration;
  for (int i=0; i<PartitionNum; i++) {
    _All_Partition.push_back(i);
  }
  std::shuffle (_All_Partition.begin(), _All_Partition.end(),
    std::default_random_engine (0xA5A5A5A5));
}

void GraphPS::init_info_col() {
  _my_col = _my_rank%VERTEXCOLNUM;
  for (int i=0; i<_num_workers; i++) {
    _col_to_ranks[i%VERTEXCOLNUM].push_back(i);
  }
  if (_my_rank == 0) {
    LOG(INFO) << "Vertex Col Num: " << VERTEXCOLNUM;
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
      DataPath = _DataPath + std::to_string(_All_Partition[j]);
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
      _Allocated_Partition.push_back(_All_Partition[i]);
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
  int64_t n = 0;
  #pragma omp parallel for num_threads(CMPNUM) reduction(+:n) schedule(static)
  for (int32_t i = _PartitionID_Start; i < _PartitionID_End; i++) {
    int32_t t_pid = _All_Partition[i];
    std::string DataPath;
    DataPath = _DataPath + std::to_string(t_pid);
    DataPath += "-";
    DataPath += std::to_string(_my_col);
    DataPath += ".edge.npy";
    char* EdgeDataNpy = load_edge(t_pid, DataPath);
    int32_t *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
    int32_t v_num = EdgeData[0];
    // int32_t e_num = EdgeData[1];
    int32_t *p = EdgeData;
    int32_t v = 0;
    int32_t l = 0;
    p += 1;
    for (int i=0; i < v_num; i++) {
      p++; v = *p; p++; l = *p;
      if (l > 0) {_VertexWhetherDest[v] = true;}
      for (int k = 0; k < l; k++) {
        p++; n++;
        _bf_pool[t_pid].insert(*p);
        _VertexWhetherAllocated[*p] = true;
      }
    }
  }
  _EdgeAllocatedNum = n;
}

void GraphPS::build_vertex_data() {
  for (VidDtype i=0; i < _VertexTotalNum; i++) {
    if (_VertexWhetherAllocated[i] == true) {
      _VertexData[i] = new VertexData();
    }
  }
}

void GraphPS::build_vertex_node() {
  VidDtype sum_of_elems = 0;
  for (int i=0; i<_VertexTotalNum; i++) {
     sum_of_elems += _VertexWhetherAllocated[i];
  }
  assert(sum_of_elems == VidDtype(_VertexData.size()));
  _VertexAllocatedNum = sum_of_elems;
  sum_of_elems = 0;
  for (int i=0; i<_VertexTotalNum; i++) {
     sum_of_elems += _VertexWhetherDest[i];
  }
  _VertexDestNum = sum_of_elems;

  _Vertex_Need_Ratio = (_VertexAllocatedNum*1.0/(_VertexTotalNum));
  bool* vertex_whether_allocated = new bool[_VertexTotalNum];
  for (int i=0; i<_num_workers; i++) {
    if (_my_rank == i) {
      memcpy(vertex_whether_allocated, _VertexWhetherAllocated, _VertexTotalNum);
    }
    MPI_Bcast(vertex_whether_allocated, _VertexTotalNum, MPI_C_BOOL, i, MPI_COMM_WORLD);
    for (int j=0; j<_VertexTotalNum; j++) {
      if (vertex_whether_allocated[j] == true) {
        _VertexToNodes[j][i] = true;
      } else {
        _VertexToNodes[j][i] = false;
      }
    }
  }
  delete[] vertex_whether_allocated;
  sum_of_elems = 0;
  for (int i=0; i<_VertexTotalNum; i++) {
    sum_of_elems += _VertexToNodes[i][_my_rank];
  }
  assert(sum_of_elems == _VertexAllocatedNum);
}

void GraphPS::init_bf() {
  _EdgeAllocatedNum = 0;
  _VertexAllocatedNum = 0;
  _VertexWhetherAllocated = new bool[_VertexTotalNum];
  _VertexWhetherDest = new bool[_VertexTotalNum];
  for (int i=0; i<_VertexTotalNum; i++) {
    _VertexToNodes.push_back(std::bitset<VERTEXROWNUM*VERTEXCOLNUM>(0));
    _VertexWhetherAllocated[i] = false;
    _VertexWhetherDest[i] = false;
  }

  _bf_parameters.projected_element_count = 1000000;
  _bf_parameters.false_positive_probability = 0.01;
  _bf_parameters.random_seed = 0xA5A5A5A5;
  if (!_bf_parameters) {assert(1==0);}
  _bf_parameters.compute_optimal_parameters();
  for (int32_t k=_PartitionID_Start; k<_PartitionID_End; k++) {
    _bf_pool[_All_Partition[k]] = bloom_filter(_bf_parameters);
  }
  build_bf();
  build_vertex_data();
  build_vertex_node();

  LOG(INFO) << "Rank " << _my_rank << " Manages " << _VertexAllocatedNum
    << " V "  << _Vertex_Need_Ratio
    << " " << _EdgeAllocatedNum << " E"
    << " Updates " << _VertexDestNum*1.0/_VertexTotalNum << " V";
  barrier_workers();
  double need_ratio_total = 0;
  MPI_Allreduce(&_Vertex_Need_Ratio, &need_ratio_total, 1, MPI_DOUBLE, MPI_SUM,  MPI_COMM_WORLD);
  _Vertex_Need_Ratio = need_ratio_total/_num_workers;
}

void GraphPS::init(std::string DataPath, const VidDtype VertexNum,
  const int32_t PartitionNum, const int32_t MaxIteration) {
  assert(_num_workers == VERTEXCOLNUM * VERTEXROWNUM);
  start_time_init();
  init_info_basic(DataPath, VertexNum, PartitionNum, MaxIteration);
  init_info_col();
  init_info_row();
  init_edge_cache();
  init_bf();
}

void GraphPS::load_vertex_outdegree(){
  std::string vout_path = _DataPath + "outdegree.npy";
  cnpy::NpyArray npz = cnpy::npy_load(vout_path);
  int32_t *data = reinterpret_cast<int32_t*>(npz.data);
  for (int i=0; i<_VertexTotalNum; i++) {
    if (_VertexWhetherAllocated[i] == true) {
      _VertexData[i]->outdegree = data[i];
    }
  }
  npz.destruct();
}

void GraphPS::run() {
  load_vertex_outdegree();
  init_vertex_data();
}

#endif
