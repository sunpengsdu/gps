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
  int32_t _PartitionNum;
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
  void init(std::string DataPath,
            const VidDtype VertexNum,
            const int32_t PartitionNum,
            const int32_t MaxIteration=10){};
  // virtual void init_vertex()=0;
  virtual void init_vertex(){};
  void run() {};
};

#endif
