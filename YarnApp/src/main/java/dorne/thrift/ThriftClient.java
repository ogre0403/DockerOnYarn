package dorne.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Iterator;
import java.util.Map;

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
    }

    public void callRPC2() throws TException {
        Map<String, String> kv = client.showContainer();
        Iterator iter = kv.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String,String> entry = (Map.Entry) iter.next();
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }

    public void callRPC3(int n) throws TException {
        client.delContainer(n);
    }

    public void close(){
        transport.close();
    }

    public static void main(String[] args) throws TException {
        int container_n = Integer.parseInt(args[2]);
        ThriftClient client = new ThriftClient(args[0], Integer.parseInt(args[1]));
//        client.callRPC(container_n);
//        client.callRPC2();
        client.callRPC3(container_n);
        client.close();
    }
}
