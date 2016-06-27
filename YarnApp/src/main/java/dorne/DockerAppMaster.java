package dorne;

import dorne.thrift.ThriftServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by 1403035 on 2016/5/12.
 */
public class DockerAppMaster {

    private static final Log LOG = LogFactory.getLog(DockerAppMaster.class);

    // Configuration
    private Configuration conf;

    private volatile boolean done;

    protected int numberContainer ;
    protected int containerMemory ;
    protected int containerCore   ;
    protected String containerCmdArgs = "";
    protected String containerType = "";

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
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();

    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    protected AtomicInteger numRequestedContainers = new AtomicInteger();

    // Counter for completed containers ( complete denotes successful or failed )
    protected AtomicInteger numCompletedContainers = new AtomicInteger();

    // Count of failed containers
    protected AtomicInteger numFailedContainers = new AtomicInteger();

    // Launch threads
    protected List<Thread> launchThreads = new ArrayList<Thread>();

    protected  Map<String, String> envs;

    // YARN-1902 , SPARK-2687, SLIDER-829, SLIDER-828
    // keep track container request, will be removed after get allocated container
    protected LinkedBlockingQueue<AMRMClient.ContainerRequest> requestList = new LinkedBlockingQueue<>();

    // Thrift RPC server, listen command from Client
    protected ThriftServer thriftServer;

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

    public boolean init(String[] args) throws ParseException, TTransportException {

        Options opts = Util.AMOptions();

        CommandLine cliParser = new GnuParser().parse(opts, args);

        // read container number or use default value 1
        numberContainer = Integer.parseInt(
                    cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, "1"));

        // read container mem configuration or use default vale 1024 MB
        containerMemory = Integer.parseInt(
                    cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM, "1024"));

        // read container cpu core configuration or use default vale 1
        containerCore = Integer.parseInt(
                    cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE, "1"));

        if(!cliParser.hasOption(DorneConst.DOREN_OPTS_DOCKER_SERVICE)){
            throw new IllegalArgumentException(
                    "Dockerized service type not specified !!");
        }
        containerType = cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_SERVICE);

        containerCmdArgs = cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_SERVICE_ARGS);

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


        // TODO
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

        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            rmClientAsync.addContainerRequest(containerAsk);
            requestList.put(containerAsk);
        }

        numRequestedContainers.set(numTotalContainersToRequest);
    }

    private void saintyCheckMemVcoreLimit(RegisterApplicationMasterResponse response){
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerMemory + ", max="
                    + maxMem);
            containerMemory = maxMem;
        }

        if (containerCore > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerCore + ", max="
                    + maxVCores);
            containerCore = maxVCores;
        }
    }

    protected AMRMClient.ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(containerCore);

        AMRMClient.ContainerRequest request =
                new AMRMClient.ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    protected boolean finish() throws IOException, YarnException {

        // wait for completion.
        while (!done && (numCompletedContainers.get() != numberContainer)) {
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
        if (numCompletedContainers.get() - numFailedContainers.get() >= numberContainer) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numberContainer
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

    public void setDone(boolean done){this.done = done;}

}
