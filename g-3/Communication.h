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

#ifndef SYSTEM_COMMUNICATION_H_
#define SYSTEM_COMMUNICATION_H_

#include "Global.h"
#include <ctime>

void zmq_send(const char * data, const int length, const int rank, const int id) {
  int  omp_id = omp_get_thread_num();
  void *requester = NULL;
  if (_Socket_Pool.find(omp_id) == _Socket_Pool.end()) {
    std::unordered_map<int, void*> _Sockets;
    _Socket_Pool[omp_id] = _Sockets;
  }
  if (_Socket_Pool[omp_id].find(rank) == _Socket_Pool[omp_id].end()) {
    std::string dst("tcp://");
    dst += std::string(_all_hostname + rank*HOST_LEN);
    dst += ":";
    dst += std::to_string(ZMQ_PORT+id);
    _Socket_Pool[omp_id][rank] = zmq_socket(_zmq_context, ZMQ_REQ);
    requester = _Socket_Pool[omp_id][rank];
    zmq_connect (requester, dst.c_str());
  }
  requester = _Socket_Pool[omp_id][rank];
  char buffer [5];
  zmq_send (requester, data, length, 0);
  zmq_recv (requester, buffer, 5, 0);
  // zmq_close (requester);
}

template<class T>
void graphps_sendall(std::vector<T>& data_vector, int32_t changed_num, std::vector<bool>& data_state, int32_t start_col, int32_t end_col) {
  int  omp_id = omp_get_thread_num();
  assert (_Send_Buffer_Lock[omp_id] == 0);
  _Send_Buffer_Lock[omp_id]++;
  int32_t length = 0;
  int32_t density = (int32_t)data_vector.back();
  char* data = NULL;
  std::vector<T> sparsedata_vector;
  sparsedata_vector.reserve(2.2*changed_num);
  int32_t changed_num_verify = 0;

  if (density < DENSITY_VALUE) {
    for (int32_t k=0; k<data_vector.size()-5; k++) {
      if (data_state[k] == true) {
        sparsedata_vector.push_back(k);
        sparsedata_vector.push_back(data_vector[k]);
        changed_num_verify++;
      }
    }
    // std::cout << changed_num_verify << ", " << changed_num << "\n";
    assert(changed_num_verify == changed_num);
    sparsedata_vector.push_back(data_vector[data_vector.size()-5]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-4]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-3]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-2]);
    sparsedata_vector.push_back(data_vector[data_vector.size()-1]);
    data = reinterpret_cast<char*>(&sparsedata_vector[0]);
    length = sizeof(T)*sparsedata_vector.size();
  } else {
    data = reinterpret_cast<char*>(&data_vector[0]);
    length = sizeof(T)*data_vector.size();
  }

  std::srand(std::time(0));
  std::vector<int32_t> random_rank;
  for (int rank=0; rank<_num_workers; rank++) {
    random_rank.push_back(rank);
  }
  std::random_shuffle(random_rank.begin(), random_rank.end());

  if (COMPRESS_NETWORK_LEVEL == 0) {
    for (int target_col = start_col; target_col <= end_col; target_col++) {
      for (int target_rank : _col_to_ranks[target_col]) {
        zmq_send(data, length, target_rank,  0);
        if (target_rank != _my_rank) {
          _Network_Uncompressed.fetch_add(length, std::memory_order_relaxed);
          _Network_Compressed.fetch_add(length, std::memory_order_relaxed);
        }
      }
    }
  } else if (COMPRESS_NETWORK_LEVEL == 1) {
    size_t max_compressed_length = snappy::MaxCompressedLength(length);
    size_t compressed_length = 0;
    if (_Send_Buffer_Len[omp_id] < max_compressed_length ||
      _Send_Buffer_Len[omp_id] > max_compressed_length*1.2) {
      if (_Send_Buffer_Len[omp_id] > 0) {delete [] (_Send_Buffer[omp_id]);}
      _Send_Buffer[omp_id] = new char[int(max_compressed_length*1.2)];
      _Send_Buffer_Len[omp_id] = int(max_compressed_length*1.2);
    }
    char *compressed_data = _Send_Buffer[omp_id];
    snappy::RawCompress(data, length, compressed_data, &compressed_length);
    for (int target_col = start_col; target_col <= end_col; target_col++) {
      for (int target_rank : _col_to_ranks[target_col]) {
        zmq_send(compressed_data, compressed_length, target_rank, 0);
        if (target_rank != _my_rank) {
          _Network_Uncompressed.fetch_add(length, std::memory_order_relaxed);
          _Network_Compressed.fetch_add(compressed_length, std::memory_order_relaxed);
        }
      }
    }
  } else if (COMPRESS_NETWORK_LEVEL > 1) {
    size_t buf_size = compressBound(length);
    int compress_result = 0;
    size_t compressed_length = 0;
    if (_Send_Buffer_Len[omp_id] < buf_size ||
      _Send_Buffer_Len[omp_id] > buf_size*1.2) {
      if (_Send_Buffer_Len[omp_id] > 0) {delete [] (_Send_Buffer[omp_id]);}
      _Send_Buffer[omp_id] = new char[int(buf_size*1.2)];
      _Send_Buffer_Len[omp_id] = int(buf_size*1.2);
    }
    char *compressed_data = _Send_Buffer[omp_id];
    compressed_length = _Send_Buffer_Len[omp_id];
    compress_result = compress2((Bytef *)compressed_data,
                              &compressed_length,
                              (Bytef *)data,
                              length,
                              5);
    assert(compress_result == Z_OK);
    for (int target_col = start_col; target_col <= end_col; target_col++) {
      for (int target_rank : _col_to_ranks[target_col]) {
        zmq_send(compressed_data, compressed_length, target_rank, 0);
        if (target_rank != _my_rank) {
          _Network_Uncompressed.fetch_add(length, std::memory_order_relaxed);
          _Network_Compressed.fetch_add(compressed_length, std::memory_order_relaxed);
        }
      }
    }
  } else {
    assert (1 == 0);
  }
  _Send_Buffer_Lock[omp_id]--;
}

template<class T>
void graphps_server_backend(std::vector<T>& VertexMsgNew, std::vector<T>& VertexMsg, int32_t id, std::vector<std::mutex>& locks) {
  void *responder = zmq_socket (_zmq_context, ZMQ_REP);
  assert(zmq_connect (responder, "inproc://graphps") == 0);
  char *buffer = new char[ZMQ_BUFFER];
  char *uncompressed_c = new char[ZMQ_BUFFER];
  size_t uncompressed_length;
  memset(buffer, 0, ZMQ_BUFFER);
  while (1) {
    // memset(buffer, 0, ZMQ_BUFFER);
    int length = zmq_recv (responder, buffer, ZMQ_BUFFER, 0);
    if (length == -1) {break;}
    _Pending_Request++;
    assert(length < ZMQ_BUFFER);
    zmq_send (responder, "ACK", 3, 0);
    if (COMPRESS_NETWORK_LEVEL == 0) {
      memcpy(uncompressed_c, buffer, length);
      uncompressed_length = length;
    } else if (COMPRESS_NETWORK_LEVEL == 1) {
      assert (snappy::RawUncompress(buffer, length, uncompressed_c) == true);
      assert (snappy::GetUncompressedLength(buffer, length, &uncompressed_length) == true);
    } else if (COMPRESS_NETWORK_LEVEL > 1) {
      int uncompress_result = 0;
      uncompressed_length = ZMQ_BUFFER*1.1;
      uncompress_result = uncompress((Bytef *)uncompressed_c,
                                    &uncompressed_length,
                                    (Bytef *)buffer,
                                    length);
      assert (uncompress_result == Z_OK);
    } else {
      assert (1 == 0);
    }
    T* raw_data = (T*) uncompressed_c;
    int32_t raw_data_len = uncompressed_length / sizeof(T);
    int32_t density = raw_data[raw_data_len-1];
    int32_t start_id = (int32_t)raw_data[raw_data_len-2]*10000 + (int32_t)raw_data[raw_data_len-3];
    int32_t partition_id = (int32_t)raw_data[raw_data_len-4];
    int32_t split_offset = (int32_t)raw_data[raw_data_len-5];

    locks[partition_id].lock();
    if (density >= DENSITY_VALUE) {
      int32_t k=0;
      int32_t k_end=0;
      int32_t id=0;
      if (get_col_id(start_id) == _my_col) {
        k = 0;
        k_end = split_offset;
      } else {
        k = split_offset;
        k_end = raw_data_len-5;
      }
      for (; k<k_end; k++) {
        id = k+start_id-Vertex_Col_StartID[_my_col];
        #ifdef PAGERANK
        VertexMsgNew[id] += raw_data[k];
        #endif
        #ifdef SSSP
        if (VertexMsgNew[id] > raw_data[k]) {
          VertexMsgNew[id] = raw_data[k];
        }
        #endif
        #ifdef CC
        if (VertexMsgNew[id] > raw_data[k]) {
          VertexMsgNew[id] = raw_data[k];
        }
        #endif
      }
    } else {
      int32_t k=0;
      int32_t k_end = (raw_data_len-5)/2;
      int32_t id=0;
      for (; k<k_end; k++) {
        id = raw_data[2*k]+start_id;
        if (get_col_id(id) != _my_col) {
          continue;
        }
        id = id-Vertex_Col_StartID[_my_col];
        #ifdef PAGERANK
        VertexMsgNew[id] += raw_data[2*k+1];
        #endif
        #ifdef SSSP
        if (VertexMsgNew[id] > raw_data[2*k+1]) {
          VertexMsgNew[id] = raw_data[2*k+1];
        }
        #endif
        #ifdef CC
        if (VertexMsgNew[id] > raw_data[2*k+1]) {
          VertexMsgNew[id] = raw_data[2*k+1];
        }
        #endif
      }
    }
    locks[partition_id].unlock();
    _Pending_Request--;
  }
}

template<class T>
void graphps_server(std::vector<T>& VertexMsgNew, std::vector<T>& VertexMsg, std::vector<std::mutex>& locks) {
  std::string server_addr(ZMQ_PREFIX);
  server_addr += std::to_string(ZMQ_PORT);
  void *server_frontend = zmq_socket (_zmq_context, ZMQ_ROUTER);
  assert (server_frontend);
  assert (zmq_bind (server_frontend, server_addr.c_str()) == 0);
  void *server_backend = zmq_socket (_zmq_context, ZMQ_DEALER);
  assert(server_backend);
  assert (zmq_bind (server_backend, "inproc://graphps") == 0);
  std::vector<std::thread> zmq_server_pool;
  for (int32_t i=0; i<ZMQNUM; i++)
    zmq_server_pool.push_back(std::thread(graphps_server_backend<T>, std::ref(VertexMsgNew), std::ref(VertexMsg), i, std::ref(locks)));
  // for (int32_t i=0; i<ZMQNUM; i++) 
  //   zmq_server_pool[i].detach();
  zmq_proxy (server_frontend, server_backend, NULL);
}

#endif /* SYSTEM_COMMUNICATION_H_ */
