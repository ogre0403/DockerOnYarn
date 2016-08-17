package dorne;

import dorne.bean.ServiceBean;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by 1403035 on 2016/5/20.
 */
public class Util {
    public static Options ClientOptions(){
        Options opts = new Options();
        opts.addOption(DorneConst.DOREN_OPTS_APPNAME, true,
                "Application Name. Default value - dorne");
        opts.addOption(DorneConst.DOREN_OPTS_YARN_AM_MEM, true,
                "Amount of memory in MB to run AM");
        opts.addOption(DorneConst.DOREN_OPTS_YARN_AM_CORE, true,
                "Amount of virtual cores to run AM");
        opts.addOption(DorneConst.DOREN_OPTS_JAR, true,
                "Jar file containing the application master");
        opts.addOption(DorneConst.DOREN_OPTS_YAML, true,
                "Dorne configuration file. Default is compose.yaml. ");
        return opts;
    }

    public static Options AMOptions(){
        Options opts = new Options();
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_APPID, true, "Application ID");
//        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, true,
//                "No. of containers on which the shell command needs to be executed");
//        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM, true,
//                "Amount of memory in MB to be requested to run docker container");
//        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE, true,
//                "Amount of core to be requested to run docker container");
//        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_SERVICE, true,
//                "Prebuild dockerized service type");
//        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_SERVICE_ARGS, true,
//                "dockerized service command arguments");

        return opts;
    }

    public static Options ThriftClientOption(){
        Options opts = new Options();
        opts.addOption("server", true, "thrift server IP");
        opts.addOption("port", true, "thrift server port");
        opts.addOption("operation", true, "scale/remove/show/shutdown");
        opts.addOption("service", true, "service name and scale number");
        return opts;
    }

    public static int getAvailablePort() throws IOException {
        ServerSocket socket = null;
        int port = 0;
        try {
            socket = new ServerSocket(0);
            port = socket.getLocalPort();
        } finally {
            if (socket!=null)
                socket.close();
        }
        return port;
    }

    /*
     * Append YARN application id to DNS Service name
     */
    public static void prefixAppIdToServiceName(ConcurrentHashMap<String, ServiceBean> compose, String appid){
        ServiceBean tmp;

        for(String k : compose.keySet()){
            tmp = compose.get(k);
            String val = tmp.getEnvironment().get("SERVICE_NAME");
            tmp.getEnvironment().put("SERVICE_NAME",appid+"-"+val);
        }
    }


    /**
     *
     */
    public static void ReplaceServiceNameVariable(ServiceBean bean, ConcurrentHashMap<String, ServiceBean> compose){
        Map<String, String> env = bean.getEnvironment();
        List<String> keys = new LinkedList<>();
        for(String key: env.keySet()){
            keys.add(key);
        }

        for(String k : keys){
            if (k.endsWith("_SERVICE_NAME") && env.get(k).startsWith("$")){
                String name = env.get(k).substring(1);
                String result = compose.get(name).getEnvironment().get("SERVICE_NAME");
                env.put(k,result+".service.consul");
            }
        }
    }

    /**
     *  Topological sort (Cormen/Tarjan algorithm).
     *  https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search
     *
     *  docker compose implementation:
     *  https://github.com/docker/compose/blob/master/compose/config/sort_services.py
     */
    public static List<String> sortServices(Map<String, ServiceBean> unSorted) throws Exception {
        List<String> sorted = new LinkedList<>();
        Set<String> temporary_marked = new HashSet<>();
        ConcurrentSet<String> unmarked = new ConcurrentSet<>();
        unmarked.addAll(unSorted.keySet());

        for(String entry: unmarked) {
            // select an unmarked node to visit
            visit(entry, sorted, temporary_marked, unmarked, unSorted);
        }
        return sorted;
    }

    private static void visit(String serviceName,
                              List<String> sortServices,
                              Set<String> temporary_marked,
                              Set<String> unmarked,
                              Map<String, ServiceBean> unSorted) throws Exception {
        // if graph is not a DAG, STOP
        if(temporary_marked.contains(serviceName)) {
            throw new Exception("Not a DAG, STOP");
        }

        // if service has not been visited yet, visit
        if(unmarked.contains(serviceName)){
            temporary_marked.add(serviceName);
            for(String dependService: getDependence(serviceName, unSorted)){
                visit(dependService,  sortServices,
                        temporary_marked, unmarked, unSorted);
            }
            temporary_marked.remove(serviceName);
            unmarked.remove(serviceName);
            //add service to head of List
            sortServices.add(serviceName);
        }
    }

    private static List<String> getDependence(String name, Map<String, ServiceBean> services){
        List<String> s =  services.get(name).getDepends_on();
        return s != null ? s:new LinkedList<String>();
    }
}
