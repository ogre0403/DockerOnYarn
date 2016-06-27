package dorne.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by 1403035 on 2016/6/3.
 */
public class ThriftClient {

    private static final Log LOG = LogFactory.getLog(ThriftClient.class);
    TTransport transport;
    TProtocol protocol ;
//    AdditionService.Client client ;
    DockerService.Client client;

    public ThriftClient(String server, int port) throws TTransportException {
        transport = new TSocket(server, port);
        transport.open();
        protocol = new TBinaryProtocol(transport);
//        client = new AdditionService.Client(protocol);
        client = new DockerService.Client(protocol);
    }

    public void callRPC(int n) throws TException {
        client.addContainer(n);
//        int result = client.add(100,200);
//        LOG.info("RPC result: "+ result);
    }

    public void close(){
        transport.close();
    }

    public static void main(String[] args) throws TException {
        int container_n = Integer.parseInt(args[2]);
        ThriftClient client = new ThriftClient(args[0], Integer.parseInt(args[1]));
        client.callRPC(container_n);
        client.close();
    }
}
