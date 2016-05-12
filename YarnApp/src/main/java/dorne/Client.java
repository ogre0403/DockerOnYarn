package dorne;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Created by 1403035 on 2016/5/12.
 */
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);

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

    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 1024;
    // Amt. of virtual cores to request for container in which shell script will be executed
    private int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;
    // Image used by docker container
    private String dockerImage = "";
    // command user by docker container
    private String containerCmd = "";



    // Command line options
    private Options opts;

    private static final String appMasterJarPath = "AppMaster.jar";


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
        opts = new Options();
        opts.addOption(DorneConst.DOREN_OPTS_APPNAME, true,
                "Application Name. Default value - DistributedShell");
        opts.addOption(DorneConst.DOREN_OPTS_YARN_AM_MEM, true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption(DorneConst.DOREN_OPTS_YARN_AM_CORE, true,
                "Amount of virtual cores to be requested to run the application master");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_IMAGE, true,
                "Images to be used by docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CMD, true,
                "Command to be executed by docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM, true,
                "Amount of memory in MB to be requested to run docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE, true,
                "Amount of core to be requested to run docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption(DorneConst.DOREN_OPTS_JAR, true,
                "Jar file containing the application master");
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
            LOG.fatal("Error running CLient", t);
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


        if(!cliParser.hasOption(DorneConst.DOREN_OPTS_DOCKER_IMAGE)){
            throw new IllegalArgumentException("No docker image specified");
        }
        dockerImage = cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_IMAGE);

        containerCmd = cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CMD,"date");

        containerMemory = Integer.parseInt(
                cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM, "1024"));
        containerVirtualCores = Integer.parseInt(
                cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE, "1"));
        numContainers = Integer.parseInt(
                cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, "1"));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

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
        amContainer.setEnvironment(buildAMEnv());

        // build command for AM
        List<String> commands = new ArrayList<String>();
        commands.add(buildAMCommand());
        amContainer.setCommands(commands);

        // setup appContext
        configAppContext(appContext, amContainer);

        // Submit the application to the applications manager
        LOG.info("Submitting application to ASM");
        yarnClient.submitApplication(appContext);

        // monitor until app is finish or killed or failed
        return monitorApplication(appId);
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

    private Map<String, LocalResource> buildAMLocalResource(ApplicationId appId) throws IOException {
        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
                localResources, null);


        /*
        if (!shellCommand.isEmpty()) {
            addToLocalResources(fs, null, shellCommandPath, appId.toString(),
                    localResources, shellCommand);
        }

        if (shellArgs.length > 0) {
            addToLocalResources(fs, null, shellArgsPath, appId.toString(),
                    localResources, StringUtils.join(shellArgs, " "));
        }
        */

        return localResources;
    }

    private Map<String, String> buildAMEnv(){
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        env.put("CLASSPATH", classPathEnv.toString());

        return env;
    }

    private String buildAMCommand(){

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("--" + DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM + " " + String.valueOf(containerMemory));
        vargs.add("--" + DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE+ " " + String.valueOf(containerVirtualCores));
        vargs.add("--" + DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM + " " + String.valueOf(numContainers));
        vargs.add("--" + DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CMD + " " + String.valueOf(containerCmd));
        vargs.add("--" + DorneConst.DOREN_OPTS_DOCKER_IMAGE + " " + String.valueOf(dockerImage));

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

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
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