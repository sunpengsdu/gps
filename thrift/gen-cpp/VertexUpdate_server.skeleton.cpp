// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "VertexUpdate.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::graphps;

class VertexUpdateHandler : virtual public VertexUpdateIf {
 public:
  VertexUpdateHandler() {
    // Your initialization goes here
  }

  int32_t ping(const int32_t id) {
    // Your implementation goes here
    printf("ping\n");
  }

  int32_t update_vertex_sparse(const int32_t pid, const VidDtype vlen, const std::vector<VidDtype> & vid, const std::vector<VmsgDtype> & vmsg) {
    // Your implementation goes here
    printf("update_vertex_sparse\n");
  }

  int32_t update_vertex_dense(const int32_t pid, const VidDtype vlen, const VidDtype start_id, const std::vector<VmsgDtype> & vmsg) {
    // Your implementation goes here
    printf("update_vertex_dense\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<VertexUpdateHandler> handler(new VertexUpdateHandler());
  shared_ptr<TProcessor> processor(new VertexUpdateProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

