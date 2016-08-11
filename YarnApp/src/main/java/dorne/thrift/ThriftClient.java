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
import java.util.List;
import java.util.Map;

public class ThriftClient {
    private static final Log LOG = LogFactory.getLog(ThriftClient.class);
    TTransport transport;
    TProtocol protocol ;
    DockerService.Client client;

    // Command line options
    private Options opts;
    private String op;
    private String service;

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
            if (!cliParser.hasOption("service")){
                throw new IllegalArgumentException("No service for client to initialize");
            }
            this.service = cliParser.getOptionValue("service");
        }
        createConn(server, port);
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
                add("");
                break;
            case "remove":
                remove(this.service);
                break;
            case "show":
                show();
                break;
            case "shutdown":
                shutdown();
                break;
            default:
                show();
        }
    }

    private void add(String scale) throws TException {
        //TODO: split scale string
        client.scaleService("", 0);
    }

    private void show() throws TException {
        List<String> result = client.showServices();
        for(String S: result){
            LOG.info(S);
        }
    }

    private void remove(String service) throws TException {
        client.removeService(service);
    }

    private void shutdown() throws TException {
        List<String> result = client.showServices();
        for(String s: result){
            // result format is nimbus_1//dreamy_mestorf@slave1:192.168.33.170
            String name = s.split("/")[0];
            LOG.info("Stop " + name +"...");
            remove(name);
        }
    }

    public void close(){
        transport.close();
    }
}
