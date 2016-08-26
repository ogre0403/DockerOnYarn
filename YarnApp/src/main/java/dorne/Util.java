package dorne;

import dorne.bean.ConfigBean;
import dorne.bean.ServiceBean;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
     * Replace docker environment variable name end with "_SERVICE_NAME", and value start with "$".
     * For example, a variable is ABC_SERVICE_NAME=$xyz, replace $xyz by service's SERVICE_NAME value,
     * which append ".service.consul"
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

    public static void removeClientModeService(ConcurrentHashMap<String, ServiceBean> compose){

        for(Map.Entry<String, ServiceBean> entry : compose.entrySet()){
            if(entry.getValue().getDeploy_mode().equals("client")){
                compose.remove(entry.getKey());
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

    /*
     * Put file into HDFS, and create localResource Map.
     * AppMaster will download localresource to localhost
     * */
    public static void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath,
                                     String appName, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);

        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }

    /*
     * Parse yaml configuration.
     * Note: snakeyaml lib must be found at Client side CLASSPATH
     * */
    public static ConcurrentHashMap<String, ServiceBean> parseComposeYAML(String file) throws IOException {
        FileInputStream fis = null;
        ConfigBean parsed;
        try {
            fis = new FileInputStream(new File(file));
            Yaml beanLoader = new Yaml(new Constructor(){
                @Override
                protected Map<Object, Object> createDefaultMap() {
                    return new ConcurrentHashMap<>();
                }
            });
            parsed = beanLoader.loadAs(fis, ConfigBean.class);
        }finally {
            if (fis != null)
                fis.close();
        }
        return (ConcurrentHashMap) parsed.getServices();
    }
}
