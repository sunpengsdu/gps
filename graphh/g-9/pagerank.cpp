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

#include "../system/GraphPS.h"

using namespace std;

template<class T>
bool comp_pagerank(const int32_t P_ID,
                   std::string DataPath,
                   const int32_t VertexNum,
                   T* VertexMsg,
                   T* VertexMsgNew,
                   const int32_t* _VertexOut,
                   const int32_t* _VertexIn,
                   const int32_t step,
                   std::vector<bool>& VertexState) {
  auto T_START = std::chrono::steady_clock::now();
  int32_t *EdgeData, *indices, *indptr;
  int32_t start_id, end_id;
  std::vector<T> result;
  std::vector<bool> result_state;
  init_comp<T>(P_ID, DataPath, &EdgeData, &start_id, &end_id, &indices, &indptr, std::ref(result), std::ref(result_state));
  int32_t i   = 0;
  int32_t k   = 0;
  int32_t tmp = 0;
  bool updated = false;
  T   rel = 0;
  int changed_num = 0;
  for (i=0; i < end_id-start_id; i++) {
    rel = 0;
    for (k = 0; k < indptr[i+1] - indptr[i]; k++) {
      tmp = indices[indptr[i] + k];
      rel += VertexMsg[tmp]/_VertexOut[tmp];
    }
    rel = rel*0.85 + 1.0/VertexNum;
    result[i] = rel;
    result_state[i]=true; //must be true all the time
    changed_num++;
    // _Changed_Vertex++;
  }
  // std::cout << changed_num << "\n";
  end_comp<T>(P_ID, EdgeData, start_id, end_id, changed_num, VertexMsg, VertexMsgNew, std::ref(result), std::ref(result_state));
  auto T_END = std::chrono::steady_clock::now();
  auto TIME =  std::chrono::duration_cast<std::chrono::milliseconds>(T_END - T_START).count();
  // LOG(INFO) << "### " << TIME;
  return true;
}

template<class T>
class PagerankPS : public GraphPS<T> {
public:
  PagerankPS():GraphPS<T>() {
    this->_comp = comp_pagerank<T>;
  }
  void init_vertex() {
    this->load_vertex_out();
    #pragma omp parallel for num_threads(this->_ThreadNum) schedule(static)
    for (int32_t i=0; i<Vertex_Col_Len[_my_rank]; i++) {
      if (this->_VertexOut[i] == 0)
        this->_VertexOut[i] = 1;
    }
    this->_VertexMsg.assign(Vertex_Col_Len[_my_rank], 1.0/this->_VertexNum);
    this->_VertexMsgNew.assign(Vertex_Col_Len[_my_rank], 0);
  }
};

int main(int argc, char *argv[]) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_workers();
  PagerankPS<double> pg;
  //PagerankPS<float> pg;
  // Data Path, VertexNum number, Partition number,  Max Iteration
  // pg.init("/data/3/mp-9/eu/", 1070557254, 800, 100);
  pg.init("/data/3/mp-9/twitter/", 41652230, 50,  100);
  // pg.init("/data/3/mp-9/uk/", 787801471, 500,  100);
  // pg.init("/data/3/mp-9/webuk/", 133633040, 100, 100);
  pg.run();
  stop_time_app();
  LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
  finalize_workers();
  return 0;
}
