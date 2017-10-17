#include "./include/global.h"
#include "graphps.h"

shared_ptr<GraphPS> _GraphPS;

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
  init_nodes();
  std::thread gserver_thread(start_gserver);
  sleep_ms(10);
  _GraphPS = boost::make_shared<GraphPS>();

  new_gworker();

  gserver_thread.join();
  return 0;
}
