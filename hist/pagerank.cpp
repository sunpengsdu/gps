#include "graphps.h"

shared_ptr<GraphPS> _GraphPS;

class VertexUpdateHandler : virtual public VertexUpdateIf {
 public:
  VertexUpdateHandler() {}

  int32_t ping(int32_t id) {
    LOG(INFO) << "ping " << id;
    return 0;
  }

  int32_t update_vertex(const VidDtype len, const std::vector<VidDtype> &vid, const std::vector<VmsgDtype> &vmsg) {
    VidDtype id = 0;
    assert(len == int(vid.size()));
    assert(len == int(vmsg.size()));
    for (int i=0; i<len; i++) {
      id = vid[i];
      while(CAS(&(_GraphPS->_VertexLock[id]), false, true) == false) {sleep_ms(1);}
      _GraphPS->_VertexData[id]->msg += vmsg[i];
      CAS(&(_GraphPS->_VertexLock[id]), true, false);
    }
    return 0;
  }
};

void GraphPS::init_vertex_data() {
  for (auto it=_VertexData.begin(); it!=_VertexData.end(); it++) {
    it->second->value = 1.0/_VertexTotalNum;
    it->second->msg = 0;
  }
}

void GraphPS::comp(int32_t P_ID) {
  int32_t *PartitionData = load_partition(P_ID);
  int32_t v_num = PartitionData[0];
  int32_t *p = PartitionData+1;
  VidDtype vid = 0;
  VidDtype len = 0;
  std::vector<VidDtype> vid_vec;
  std::vector<VmsgDtype> vmsg_vec;
  VmsgDtype vmsg = 0;
  for (int i=0; i < v_num; i++) {
    p++; vid = *p; p++; len = *p;
    vmsg = 0;
    if (len==0) {continue;} 
      vid_vec.push_back(vid);
    for (int k=0; k < len; k++) {
      p++;
      vmsg += _VertexData[*p]->value/_VertexData[*p]->outdegree;
    }
    vmsg_vec.push_back(vmsg);
  }
  clean_edge(P_ID, reinterpret_cast<char*>(PartitionData));
  // send_msg(std::ref(vid_vec), std::ref(vmsg_vec));
}

void GraphPS::pre_process() {
  BF_THRE = 0; //force to activate all partitions
}

void GraphPS::post_process() {
  #pragma omp parallel for num_threads(CMPNUM) schedule(static)
  for (int i=0; i<_VertexTotalNum; i++) {
    if (_VertexWhetherAllocated[i]==true) {
      VvalueDtype value  = 0.85*_VertexData[i]->msg + 1.0/_VertexTotalNum;
      if (_VertexData[i]->value == value) {
        _VertexData[i]->state = false;
      } else {
        _VertexData[i]->value = value;
        _VertexData[i]->state = true;
      }
      _VertexData[i]->msg = 0;
    }
  }
}

void start_gserver() {
  shared_ptr<VertexUpdateHandler> handler(new VertexUpdateHandler());
  shared_ptr<TProcessor> processor(new VertexUpdateProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(_server_port));
  #ifdef COMPRESS
  shared_ptr<TTransportFactory> transportFactory(new TZlibBufferdTransportFactory());
  #else
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  #endif
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(COMNUM);
  shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<PosixThreadFactory>(new PosixThreadFactory());    
  threadManager->threadFactory(threadFactory);    
  threadManager->start();    
  TThreadPoolServer gserver(processor, serverTransport, transportFactory, protocolFactory, threadManager);
  gserver.serve();
}

int main(int argc, char **argv) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_nodes();
  std::thread gserver_thread(start_gserver);
  sleep_ms(10);
  _GraphPS = boost::make_shared<GraphPS>();
  // Data Path, VertexNum number, Partition number,  Max Iteration

  // _GraphPS->init("/data/3/gps-1/twitter/", 41652230, 450, 200);
  // _GraphPS->init("/data/3/gps-1/webuk/", 133633040, 900, 5);
  // _GraphPS->init("/data/3/gps-1/uk/",    787801471, 4500, 5);
  // _GraphPS->init("/data/3/gps-1/eu/", 1070557254, 5400, 5);

  // _GraphPS->init("/data/3/gps-9/twitter/", 41652230, 50, 200);
  _GraphPS->init("/data/3/gps-9/webuk/", 133633040, 100, 200);
  // _GraphPS->init("/data/3/gps-9/uk/",    787801471, 500, 200);
  // _GraphPS->init("/data/3/gps-9/eu/", 1070557254, 600, 200);
  sleep_ms(10);

  _GraphPS->run();

  gserver_thread.join();
  stop_time_app();
  return 0;
}
