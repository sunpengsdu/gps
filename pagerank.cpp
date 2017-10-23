#include "graphps.h"

shared_ptr<GraphPS> _GraphPS;

void GraphPS::init_vertex_data() {

}

void GraphPS::comp(int32_t P_ID) {

}

void GraphPS::post_process() {
  BF_THRE = 0;
}

class VertexUpdateHandler : virtual public VertexUpdateIf {
 public:
  VertexUpdateHandler() {}
  int32_t ping(int32_t id) {
    LOG(INFO) << "ping " << id;
    return 0;
  }
};

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

void new_gworker() {
  shared_ptr<TTransport> socket(new TSocket("localhost", _server_port));
  #ifdef COMPRESS
  shared_ptr<TTransport> bufferdtransport(new TBufferedTransport(socket));
  shared_ptr<TZlibTransport> transport(new TZlibTransport(bufferdtransport, 
    DEFAULT_URBUF_SIZE_SP,  DEFAULT_CRBUF_SIZE_SP, 
    DEFAULT_UWBUF_SIZE_SP, DEFAULT_CWBUF_SIZE_SP, _comp_level));
  #else
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  #endif
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

  shared_ptr<VertexUpdateClient> client(new VertexUpdateClient(protocol));
  try {
  transport->open();
  int32_t i = 1;
  int32_t j = 0;
  j = client->ping(i);
  std::cout << j << "\n";
  transport->close();
  } catch (TException& tx) {
      std::cout << "ERROR: " << tx.what() << std::endl;
  }
}

int main(int argc, char **argv) {
  start_time_app();
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  init_nodes();
  // std::thread gserver_thread(start_gserver);
  sleep_ms(10);
  _GraphPS = boost::make_shared<GraphPS>();
  // Data Path, VertexNum number, Partition number,  Max Iteration

  _GraphPS->init("/home/mapred/twitter.chaos-graphps/", 41652230, 450, 5);
  // _GraphPS->init("/home/mapred/webuk.chaos-graphps/", 133633040, 900, 5);

  _GraphPS->run();


  // _GraphPS->init("/home/mapred/ps-9/webuk.chaos-graphps/", 133633040, 100, 2000);
  // _GraphPS->init("/home/mapred/ps-9/twitter.chaos-graphps/twitter/", 41652230, 50, 2000);

  //new_gworker();

  // gserver_thread.join();
  stop_time_app();
  return 0;
}
