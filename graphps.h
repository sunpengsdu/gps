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
  int32_t _PartitionTotalNum;
  int32_t _MaxIteration;
  int32_t _PartitionID_Start;
  int32_t _PartitionID_End;
  VidDtype _VertexID_Start;
  VidDtype _VertexID_End;
  std::vector<int32_t> _Allocated_Partition;
  std::vector<bool> _Vertex_State;
  std::unordered_map<VidDtype, VertexData> _VertexData;
  bloom_parameters _bf_parameters;
  std::map<int32_t, bloom_filter> _bf_pool;
  void init_info_basic(std::string DataPath, const VidDtype VertexNum,
    const int32_t PartitionNum, const int32_t MaxIteration=10);
  void init_info_col();
  void init_info_row();
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
      //due to history issues
      if (VERTEXCOLNUM > 1) {
        DataPath += "-";
        DataPath += std::to_string(i);
      }
      DataPath += ".edge.npy";
      total_size_col += get_file_size(DataPath.c_str());
    }
    avg_size_row = total_size_col*1.0/VERTEXROWNUM;
    for (int j=0; j<_PartitionTotalNum; j++) {
      std::string DataPath;
      DataPath = _DataPath + std::to_string(j);
      //due to history issues
      if (VERTEXCOLNUM > 1) {
        DataPath += "-";
        DataPath += std::to_string(i);
      }
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
  LOG(INFO) << "Rank: " << _my_rank << ", " 
    << "Row: " << _my_row << ", " 
    << "Col: " << _my_col << ", " 
    << "Partition From " << _PartitionID_Start << ", to " << _PartitionID_End;
  barrier_workers();
}

void GraphPS::init(std::string DataPath, const VidDtype VertexNum,
  const int32_t PartitionNum, const int32_t MaxIteration) {
  assert(_num_workers == VERTEXCOLNUM * VERTEXROWNUM);
  start_time_init();
  init_info_basic(DataPath, VertexNum, PartitionNum, MaxIteration);
  init_info_col();
  init_info_row();
}


#endif
