package dorne;

import dorne.bean.ConfigBean;
import dorne.bean.ServiceBean;
import dorne.thrift.ThriftServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DockerAppMaster {

    private static final Log LOG = LogFactory.getLog(DockerAppMaster.class);

    // Configuration
    private Configuration conf;

    private volatile boolean done;

    // TODO: replace with numRequestedContainers
    protected int numberContainer ;

    // Hostname of the am container
    private String appMasterHostname = "";

    // Handle to communicate with the Resource Manager
    private AMRMClientAsync rmClientAsync;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;

    // Listen to process the response from the Node Manager
    protected NMCallbackHandler containerListener;

    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    private AtomicInteger numAllocatedContainers = new AtomicInteger();

    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();

    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();

    // Launch threads
    protected List<Thread> launchThreads = new ArrayList<Thread>();

    protected  Map<String, String> envs;

    // YARN-1902 , SPARK-2687, SLIDER-829, SLIDER-828
    // keep track container request, will be removed after get allocated container
    protected LinkedBlockingQueue<AMRMClient.ContainerRequest> requestList = new LinkedBlockingQueue<>();

    // Thrift RPC server, listen command from Client
    protected ThriftServer thriftServer;

    // Keeping <dockerContainerID, Host>, docker container on which host
    private ConcurrentHashMap<String,String> dockerContainerMap = new ConcurrentHashMap<>();

    // <ServiceName, dockerContainerID>, Mapping between service name in yaml file and dockerContainerID
    private ConcurrentHashMap<String,String> serviceContainerMap = new ConcurrentHashMap<>();

    // Mapping between service name and service configuration from yaml file.
    private ConcurrentHashMap<String, ServiceBean> composeConfig ;

    // service name will removed after being launched
    private List<String> sortedServiceName;

    // prefix of service name, use to avoid service name conflict in a yarn application
    private String Appid;

    public DockerAppMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    public static void main(String[] args) {
        boolean result = false;
        try {
            DockerAppMaster appMaster = new DockerAppMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    public boolean init(String[] args) throws Exception {
        CommandLine cliParser = new GnuParser().parse(Util.AMOptions(), args);
        if (!cliParser.hasOption(DorneConst.DOREN_OPTS_DOCKER_APPID)) {
            throw new IllegalArgumentException(
                    "No application id specified");
        }
        this.Appid = cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_APPID);
        composeConfig = parseComposeYAML(DorneConst.DOREN_LOCALRESOURCE_YAML);

        Util.prefixAppIdToServiceName(composeConfig, this.Appid);

        for(ServiceBean sb: composeConfig.values()){
            Util.ReplaceServiceNameVariable(sb,composeConfig);
        }

        sortedServiceName = Util.sortServices(composeConfig);
        // read container number
        numberContainer = composeConfig.size();

        envs = System.getenv();

        try {
            int freeport= Util.getAvailablePort();
            thriftServer = new ThriftServer(freeport, this);
        } catch (IOException e) {
            e.printStackTrace();
            return  false;
        }

        return true;
    }

    public void run() throws IOException, YarnException, InterruptedException {

        LOG.info("Starting DockerAppMaster");

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler(this);
        // Too small heartbeat interval, e.g, 1000ms, will results request more container
        // at AM startup
        rmClientAsync = AMRMClientAsync.createAMRMClientAsync(10000, allocListener);
        rmClientAsync.init(conf);
        rmClientAsync.start();

        containerListener = new NMCallbackHandler(this);
        nmClientAsync = NMClientAsync.createNMClientAsync(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Setup local RPC Server to accept status requests directly from clients
        new Thread(thriftServer).start();

        // Register self with ResourceManager. This will start heartbeating to the RM
        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = rmClientAsync
                .registerApplicationMaster(appMasterHostname, thriftServer.getPort(), "http://abc/");

        saintyCheckMemVcoreLimit(response);

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info("Received " + previousAMRunningContainers.size()
                + " previous AM's running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        int numTotalContainersToRequest =
                numberContainer - previousAMRunningContainers.size();

        Thread.sleep(5000);

        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            rmClientAsync.addContainerRequest(containerAsk);
            requestList.put(containerAsk);
        }

        numRequestedContainers.set(numTotalContainersToRequest);
    }

    private  ConcurrentHashMap<String, ServiceBean> parseComposeYAML(String file) throws IOException {
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

    private void saintyCheckMemVcoreLimit(RegisterApplicationMasterResponse response){

        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
        if (DorneConst.DOREN_YARN_CONTAINER_MEM > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + DorneConst.DOREN_YARN_CONTAINER_MEM + ", max="
                    + maxMem);
            DorneConst.DOREN_YARN_CONTAINER_MEM = maxMem;
        }

        if (DorneConst.DOREN_YARN_CONTAINER_CORE > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + DorneConst.DOREN_YARN_CONTAINER_CORE + ", max="
                    + maxVCores);
            DorneConst.DOREN_YARN_CONTAINER_CORE = maxVCores;
        }
    }

    protected AMRMClient.ContainerRequest setupContainerAskForRM() {
        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Records.newRecord(Resource.class);

        //TODO: setup yarn container memory and cpu from compose.yaml
        capability.setMemory(DorneConst.DOREN_YARN_CONTAINER_MEM);
        capability.setVirtualCores(DorneConst.DOREN_YARN_CONTAINER_CORE);

        AMRMClient.ContainerRequest request =
                new AMRMClient.ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    protected boolean finish() throws IOException, YarnException {

        // wait for completion.
        while (!done && (numCompletedContainers.get() != numRequestedContainers.get())) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {}
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numCompletedContainers.get() - numFailedContainers.get() >= numRequestedContainers.get()) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numRequestedContainers.get()
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }

        try {
            rmClientAsync.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        rmClientAsync.stop();

        return success;

    }


    // helper Setter/Getter
    public NMClientAsync getNMClientAsync() { return this.nmClientAsync;}

    public AMRMClientAsync getRMClientAsync(){return this.rmClientAsync;}

    public LinkedBlockingQueue getRequestList(){
        return  requestList;
    }

    public NMCallbackHandler getContainerListener(){
        return containerListener;
    }

    public ConcurrentHashMap getDockerContainerMap(){
        return dockerContainerMap;
    }

    public Map<String, String> getEnvs(){
        return envs;
    }

    public AtomicInteger getNumAllocatedContainers(){
        return numAllocatedContainers;
    }

    public AtomicInteger getNumRequestedContainers(){
        return numRequestedContainers;
    }

    public AtomicInteger getNumCompletedContainers(){
        return numCompletedContainers;
    }

    public AtomicInteger getNumFailedContainers(){
        return numFailedContainers;
    }

    public ConcurrentHashMap<String, ServiceBean> getComposeConfig(){
        return composeConfig;
    }

    public List<String> getSortedServiceName(){
        return sortedServiceName;
    }

    public ConcurrentHashMap<String, String> getServiceContainerMap() {
        return serviceContainerMap;
    }

    public String getAppid(){
        return this.Appid;
    }

    public void setDone(boolean done){this.done = done;}

}
