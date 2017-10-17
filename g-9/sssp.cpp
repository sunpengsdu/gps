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
bool comp_sssp(const int32_t P_ID,
               std::string DataPath,
               const int32_t VertexNum,
               T* VertexMsg,
               T* VertexMsgNew,
               const int32_t* _VertexOut,
               const int32_t* _VertexIn,
               const int32_t step,
               std::vector<bool>& VertexState) {
  int32_t *EdgeData, *indices, *indptr;
  int32_t start_id, end_id;
  std::vector<T> result;
  std::vector<bool> result_state;
  init_comp<T>(P_ID, DataPath, &EdgeData, &start_id, &end_id, &indices, &indptr, std::ref(result), std::ref(result_state));
  // LOG(INFO) << end_id << " " << start_id;
  int32_t i   = 0;
  int32_t j   = 0;
  T   min = 0;
  int32_t changed_num = 0;
  T tmp;
  bool updated = false;

  for (i = 0; i < end_id-start_id; i++) {
    min = GPS_INF;
    for (j = 0; j < indptr[i+1] - indptr[i]; j++) {
      tmp = VertexMsg[indices[indptr[i] + j]] + 1;
      // if (_my_rank == 0 && indices[indptr[i]+j] == 0) {
         // std::cout << _my_rank << " ###\n";
      // }
      if (min > tmp)
        min = tmp;
      if (VertexState[indices[indptr[i] + j]]) 
        updated = true;
    }
    result[i] = min;
    if (updated) {
      changed_num++;
      result_state[i] = true;
    }
  }
  end_comp<T>(P_ID, EdgeData, start_id, end_id, changed_num, VertexMsg, VertexMsgNew, std::ref(result), std::ref(result_state));
  return true;
}

template<class T>
class PagerankPS : public GraphPS<T> {
public:
  PagerankPS():GraphPS<T>() {
    this->_comp = comp_sssp<T>;
  }
  void init_vertex() {
    this->_VertexMsg.assign(Vertex_Col_Len[_my_rank], GPS_INF);
    int init_id = 1;
    int col_id = get_col_id(init_id);
    // std::cout << col_id << ", " << _my_rank << "\n";
    int offset = init_id - Vertex_Col_StartID[col_id];
    if (_my_rank == col_id) {
      this->_VertexMsg[offset] = 0;
      // std::cout << "##########\n";
    }
    this->_VertexMsgNew.assign(this->_VertexMsg.begin(), this->_VertexMsg.end());
  }
};

int main(int argc, char *argv[]) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_workers();
  PagerankPS<int32_t> pg;
  //PagerankPS<float> pg;
  // Data Path, VertexNum number, Partition number, thread number, Max Iteration
  // pg.init("/data/3/mp-9/eu/", 1070557254, 800, 100);
  // pg.init("/data/3/mp-9/twitter/", 41652230, 50,  100);
  // pg.init("/data/3/mp-9/uk/", 787801471, 500,  100);
  pg.init("/data/3/mp-9/webuk/", 133633040, 100, 100);
  pg.run();
  stop_time_app();
  LOG(INFO) << "Used " << APP_TIME/1000.0 << " s";
  finalize_workers();
  return 0;
}
