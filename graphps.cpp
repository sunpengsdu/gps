#include "./include/global.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;  

using boost::shared_ptr;

using namespace  ::graphps;

class VertexUpdateHandler : virtual public VertexUpdateIf {
 public:
  VertexUpdateHandler() {
    // Your initialization goes here
  }

  void ping() {
    // Your implementation goes here
    printf("ping\n");
  }

};

void g_server() {
  shared_ptr<VertexUpdateHandler> handler(new VertexUpdateHandler());
  shared_ptr<TProcessor> processor(new VertexUpdateProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(_server_port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  // shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(_server_threadnum);
  // shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<PosixThreadFactory>(new PosixThreadFactory());    
  // threadManager->threadFactory(threadFactory);    
  // threadManager->start();    
  // TThreadPoolServer tserver(processor, serverTransport, transportFactory, protocolFactory, threadManager);
  TSimpleServer tserver(processor, serverTransport, transportFactory, protocolFactory);
  LOG(INFO) << "Rank: " << _my_rank << " start server";
  tserver.serve();
}

int main(int argc, char **argv) {
  init_nodes();
  std::thread graphps_server(g_server);

  return 0;
}
