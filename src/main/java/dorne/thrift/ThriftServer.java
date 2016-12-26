package dorne.thrift;

import dorne.DockerAppMaster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by 1403035 on 2016/6/3.
 */
public class ThriftServer implements Runnable{
    private static final Log LOG = LogFactory.getLog(ThriftServer.class);

    DockerAppMaster dockerAppMaster;
    TServer server;
    int port;

    public ThriftServer(int port, DockerAppMaster dockerAppMaster) throws TTransportException {
        this.port = port;
        this.dockerAppMaster = dockerAppMaster;
        TServerTransport serverTransport = new TServerSocket(this.port);
        server = new TSimpleServer(
                new TServer.Args(serverTransport).processor(
                        new DockerService.Processor(new DockerServiceHandler(this.dockerAppMaster))
                ));

//        server = new TSimpleServer(
//                new TServer.Args(serverTransport).processor(
//                        new AdditionService.Processor<AdditionServiceHandler>(new AdditionServiceHandler())
//                ));

        /*
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(port);
        THsHaServer.Args server_args = new THsHaServer.Args(serverTransport).
                processor(new AdditionService.Processor<AdditionServiceHandler>(new AdditionServiceHandler()));
        server = new THsHaServer(server_args);
        */

        // Use this for a multithreaded server
        // TServer server = new TThreadPoolServer(new
        // TThreadPoolServer.Args(serverTransport).processor(processor));
    }

    public int getPort(){
        return port;
    }

    @Override
    public void run() {
        LOG.info("Starting thrift RPC server...");
        server.serve();
    }
}
