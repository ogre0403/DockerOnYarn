package dorne;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.bean.HostInfo;
import dorne.bean.ServiceBean;
import dorne.thrift.ThriftClient;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
    protected static final Logger LOG = LoggerFactory.getLogger(Client.class);

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;

    // Main class to invoke application master
    private final String appMasterMainClass;

    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 1024;
    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;
    // Application master jar file
    private String appMasterJar = "";

    private String composeYAML = "";

    // Command line options
    private Options opts;

    private static final String appMasterJarPath = "AppMaster.jar";

    private static final String composeYAMLPath = "compose.yaml";

    public Client() throws Exception  {
        this(new YarnConfiguration());
    }

    public Client(Configuration conf) throws Exception  {
        this(DorneConst.DORNE_APPLICATION_MASTER_CLASS, conf);
    }

    Client(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = Util.ClientOptions();
    }



    public static void main(String[] args) {
        boolean result = false;
        try {
            Client client = new Client();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.error("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        appName = cliParser.getOptionValue(DorneConst.DOREN_OPTS_APPNAME, "dorne");
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue(DorneConst.DOREN_OPTS_YARN_AM_MEM, "1024"));
        amVCores = Integer.parseInt(cliParser.getOptionValue(DorneConst.DOREN_OPTS_YARN_AM_CORE, "1"));


        if (amMemory < 0) {
            throw new IllegalArgumentException(
                    "Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException(
                    "Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption(DorneConst.DOREN_OPTS_JAR)) {
            throw new IllegalArgumentException(
                    "No jar file specified for application master");
        }
        appMasterJar = cliParser.getOptionValue("jar");

        composeYAML = cliParser.getOptionValue("yaml", "compose.yaml");

        return true;
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("Running Client");
        yarnClient.start();

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // check max memory and vcore limit
        saintyCheckMemVcoreLimit(appResponse);

        // Set local resource info into app master container launch context
        amContainer.setLocalResources(buildAMLocalResource(appId));

        // set environment veriable for AM
        Map<String, String> env = new HashMap<String, String>();
        buildAMEnv(env);
//        buildAMEnvForShell(env, appId);
        amContainer.setEnvironment(env);

        // build command for AM
        List<String> commands = new ArrayList<String>();
        commands.add(buildAMCommand(appId.getId()));
        amContainer.setCommands(commands);

        // setup appContext
        configAppContext(appContext, amContainer);

        // Submit the application to the applications manager
        LOG.info("Submitting application to ASM");
        yarnClient.submitApplication(appContext);


        ConcurrentHashMap<String, ServiceBean> service = Util.parseComposeYAML(composeYAML);
        Util.prefixAppIdToServiceName(service, String.valueOf(appId.getId()));

        for(ServiceBean sb: service.values()){
            Util.ReplaceServiceNameVariable(sb,service);
        }

        List<ServiceBean> clientMode = clientModeService(service);
        HostInfo host = getAMInfo(appId);
        waitForContainerStartup(host.getIp(), host.getPort(), service.size() - clientMode.size());
        startClientModeService(clientMode);


        // monitor until app is finish or killed or failed
//        return monitorApplication(appId);
        return true;
    }

    private void saintyCheckMemVcoreLimit(GetNewApplicationResponse appResponse){
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }
    }

    private void configAppContext(ApplicationSubmissionContext appContext,
                                  ContainerLaunchContext amContainer){
        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        appContext.setApplicationName(appName);
        appContext.setApplicationType(DorneConst.DORNE_APPLICATION_TYPE);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        capability.setVirtualCores(amVCores);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

    }

    /*
    * Copy resource from localhost to HDFS, and set local resources for the application master
    * local files or archives as needed.
    * In this scenario, application master jar and yaml file are required.
    * */
    private Map<String, LocalResource> buildAMLocalResource(ApplicationId appId) throws IOException {
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        FileSystem fs = FileSystem.get(conf);
        Util.addToLocalResources(fs, appMasterJar, appMasterJarPath, appName, appId.toString(), localResources, null);

        LOG.info("Copy yaml file from local filesystem and add to local environment");
        Util.addToLocalResources(fs, composeYAML, composeYAMLPath, appName, appId.toString(), localResources, null);
        return localResources;
    }

    /*
    *  Put shell script file into HDFS, and save the file path in env Map.
    *  CLILuancher will use env Map to download shell script file and add
    *  localresource.
    * */
    /*
    private void buildAMEnvForShell(Map<String, String> env, ApplicationId appId) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        // The shell script has to be made available on the final container(s) where it will be executed.
        // To do this, we need to first copy into the hdfs that is visible to the yarn framework.
        String hdfsShellScriptLocation = "";
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        if (!containerType.isEmpty()) {
            InputStream is;

            switch (containerType) {
                case "ping":
                    is = getClass().getResourceAsStream("/demo/ping.sh");
                    break;
                case "nginx":
                    is = getClass().getResourceAsStream("/demo/nginx.sh");
                    break;
                default:
                    is = getClass().getResourceAsStream("/demo/attach.sh");
            }
            String shellPathSuffix = appName + "/" + appId.toString() + "/" + DorneConst.DOREN_DEMO_FILE;
            Path shellDst = new Path(fs.getHomeDirectory(), shellPathSuffix);
            // copy into HDFS
            OutputStream os = fs.create(shellDst);
            org.apache.hadoop.io.IOUtils.copyBytes(is,    os,    conf);
            is.close();
            os.close();

            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();


            // put location of shell script into env
            // using the env info, the application master will create the correct local resource for the
            // eventual containers that will be launched to execute the shell scripts
            env.put(DorneConst.DOREN_DEMO_SCRIPTLOCATION, hdfsShellScriptLocation);
            env.put(DorneConst.DOREN_DEMO_SCRIPTTIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
            env.put(DorneConst.DOREN_DEMO_SCRIPTLEN, Long.toString(hdfsShellScriptLen));
        }
    }
    */

    /**
     * Set the environment variable for execute application master.
     * Most important variable is CLASSPATH.
     * CLASSPATH has to contain:
     *    - Hadoop configuration folder : append HADOOP_HOME
     *    - dorne required JAR : append
     *    - AppMaster jar : append "./*"
     * */
    private void buildAMEnv(Map<String, String> env) throws IOException {
        LOG.info("Set the environment for the application master");
        StringBuilder classPathEnv = new StringBuilder();
        classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                .append("./*")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                .append("/opt/hadoop/etc/hadoop")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                .append("/home/hadoop/dorne_api/*");

        env.put("CLASSPATH", classPathEnv.toString());

//        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("/opt/hadoop/etc/hadoop");
//        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("/home/hadoop/dorne_api/*");
//        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("/home/hadoop/yarn_api/*");

        /*
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        */
    }

    /*
    * Build AM launch command
    * */
    private String buildAMCommand(int appid){

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(appMasterMainClass);
        vargs.add("--"+DorneConst.DOREN_OPTS_DOCKER_APPID);
        vargs.add(appid + "");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        LOG.info("Completed setting up app master command " + command.toString());
        return command.toString();
    }


    private HostInfo getAMInfo(ApplicationId appId) throws IOException, YarnException {
        ApplicationReport report;
        while (true) {
            report = yarnClient.getApplicationReport(appId);
            if (!report.getHost().equals("N/A")){
                LOG.info("AM: {} ", report.getHost());
                LOG.info("AM rpc port: {} ", report.getRpcPort());
                break;
            }
            try {
                LOG.info("wait for connect to RM...");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }
        }
        HostInfo hi = new HostInfo();
        hi.setHostname(report.getHost().split("/")[0]);
        hi.setIp(report.getHost().split("/")[1]);
        hi.setPort(String.valueOf(report.getRpcPort()));
        return hi;
    }

    /**
     * Connect to AM, and query how many docker container is up
     */
    private void waitForContainerStartup(String ip, String port, int n)  {

        ThriftClient TClient;

        try {
            TClient = new ThriftClient(ip, port);
            int s = 0;
            do{
                LOG.info("Wait for {}/{} Cluster Mode Container up...",s,n);
                Thread.sleep(2000);
            }while( ( s =TClient.getClusterModeContainerNum()) < n);

        } catch (TTransportException e) {
            e.printStackTrace();
            return;
        } catch (TException e) {
            e.printStackTrace();
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }
    }

    /*
     * Return service name of docker container which will be start at client instead of cluster
     **/
    private List<ServiceBean> clientModeService(ConcurrentHashMap<String, ServiceBean> service ) throws IOException {
        LinkedList<ServiceBean> list = new LinkedList<>();
        for( ServiceBean entry : service.values()) {
            if (entry.getDeploy_mode().equals("client"))
                list.add(entry);
        }
        return list;
    }

    private void startClientModeService(List<ServiceBean> services) throws IOException {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://127.0.0.1:" + DorneConst.DOREN_DOCKERHOST_PORT)
                .build();
        DockerClient docker = DockerClientBuilder.getInstance(config).build();

        for(ServiceBean s: services){
            String short_id = s.createContainer(docker).getId().substring(0,8);
            LOG.info("container id: {}", short_id);
            LOG.info("Use \"docker start -ai {}\" to run", short_id);
        }
        docker.close();
    }

    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }
        }
    }

    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
}
