package dorne.thrift;

import dorne.Util;
import org.apache.commons.cli.*;
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

public class ThriftClient {
    private static final Log LOG = LogFactory.getLog(ThriftClient.class);
    TTransport transport;
    TProtocol protocol ;
    DockerService.Client client;

    // Command line options
    private Options opts;
    private String op;
    private int num;

    public ThriftClient(String[] args) throws TTransportException {
        opts = Util.ThriftClientOption();
        init(args);
    }

    private void init(String[] args) throws  TTransportException {
        if (args.length == 0) {
            new HelpFormatter().printHelp("Client", opts);
            return;
        }

        CommandLine cliParser = null;
        try {
            cliParser = new GnuParser().parse(opts, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if(!cliParser.hasOption("server") || !cliParser.hasOption("port")){
            throw new IllegalArgumentException("No IP/Port specified for client to connect");
        }
        String server = cliParser.getOptionValue("server");
        int port = Integer.parseInt(cliParser.getOptionValue("port"));

        this.op = cliParser.getOptionValue("operation", "show");

        if(op.equals("add") || op.equals("remove")){
            if (!cliParser.hasOption("num")){
                throw new IllegalArgumentException("No container num for client to initialize");
            }
            this.num = Integer.parseInt(cliParser.getOptionValue("num"));
        }
        createConn(server,port);
    }

    private void createConn(String server, int port) throws TTransportException {
        transport = new TSocket(server, port);
        transport.open();
        protocol = new TBinaryProtocol(transport);
        client = new DockerService.Client(protocol);
    }

    public static void main(String[] args) throws TException {
        ThriftClient client = new ThriftClient(args);
        client.do_work();
        client.close();
    }

    private void do_work() throws TException {
        switch (op) {
            case "add":
                add(this.num);
                break;
            case "remove":
                remove(this.num);
                break;
            case "show":
                show();
                break;
            default:
                show();
        }
    }

    private void add(int n) throws TException {
        client.addContainer(n);
    }

    private void show() throws TException {
        Map<String, String> kv = client.showContainer();
        Iterator iter = kv.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String,String> entry = (Map.Entry) iter.next();
            LOG.info(entry.getKey().substring(0,10) + "@" + entry.getValue());
        }
    }

    private void remove(int n) throws TException {
        client.delContainer(n);
    }

    public void close(){
        transport.close();
    }
}
