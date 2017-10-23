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
#ifndef SYSTEM_COMP_H_
#define SYSTEM_COMP_H_

#include "Global.h"
#include "Communication.h"

template<class T>
bool init_comp(const int32_t& P_ID,
               std::string DataPath,
               int32_t** EdgeData,
               int32_t* start_id,
               int32_t* end_id,
               int32_t** indices,
               int32_t** indptr,
               std::vector<T>& result,
               std::vector<bool>& result_state) {
  int32_t indices_len;
  int32_t indptr_len;
   _Computing_Num++;
  DataPath += std::to_string(P_ID);
  DataPath += "-";
  DataPath += std::to_string(_my_col);
  DataPath += ".edge.npy";
  char *EdgeDataNpy = load_edge(P_ID, DataPath);
  *EdgeData = reinterpret_cast<int32_t*>(EdgeDataNpy);
  *start_id = (*EdgeData)[3];
  *end_id = (*EdgeData)[4];
  indices_len = (*EdgeData)[1];
  indptr_len = (*EdgeData)[2];
  *indices = (*EdgeData) + 5;
  *indptr = (*EdgeData) + 5 + indices_len;
  result.assign(*end_id-*start_id+5, 0);
  result_state.assign(*end_id-*start_id, false);
  result[*end_id-*start_id+4] = 0; //sparsity ratio
  result[*end_id-*start_id+3] = (int32_t)std::floor(*start_id*1.0/10000);
  result[*end_id-*start_id+2] = (int32_t)*start_id%10000;
  result[*end_id-*start_id+1] = (int32_t)P_ID;
  result[*end_id-*start_id+0] = (int32_t)0; //reserved for col splitter
  // result[*end_id-*start_id+1] = (int32_t)std::floor(*end_id*1.0/10000);
  // result[*end_id-*start_id+0] = (int32_t)*end_id%10000;
  return true;
}

template<class T>
bool end_comp(const int32_t& P_ID,
              int32_t*  EdgeData,
              int32_t   start_id,
              int32_t   end_id,
              int32_t   changed_num,
              T*        VertexMsg,
              T*        VertexMsgNew,
              std::vector<T>& result,
              std::vector<bool>& result_state) {
  clean_edge(P_ID, reinterpret_cast<char*>(EdgeData));
  
  if (get_col_id(start_id) != get_col_id(end_id-1)) {
    // std::cout << vertex_num_per_col << ", " << start_id << ", " << get_col_id(start_id) << ", "<< end_id << ", " << get_col_id(end_id) << "\n";
    result[end_id-start_id] = Vertex_Col_StartID[get_col_id(end_id-1)]-start_id;
    // std::cout << result[end_id-start_id] << "\n";
    assert(result[end_id-start_id] > 0);
    assert(result[end_id-start_id] <= end_id-start_id);
  } else {
    result[end_id-start_id] = end_id-start_id;
  }

  result[end_id-start_id+4] = (int32_t)changed_num*100.0/(end_id-start_id); //sparsity ratio
  _Computing_Num--;
  if (changed_num > 0)
    graphps_sendall<T>(std::ref(result), changed_num, std::ref(result_state), get_col_id(start_id), get_col_id(end_id-1));
}
#endif
