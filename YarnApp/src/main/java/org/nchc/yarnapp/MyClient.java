package org.nchc.yarnapp;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by superorange on 10/19/15.
 */
public class MyClient {
    private YarnClient _yarn;
    private YarnConfiguration _hadoopConf;
    private String _appMasterMainClass;
    private ApplicationId _appId;

    public MyClient(String appMasterMainClass) {
        _appMasterMainClass = appMasterMainClass;
        _hadoopConf = new YarnConfiguration();

        //The first step that a client needs to do is to initialize and start a YarnClient.
        _yarn = YarnClient.createYarnClient();
        _yarn.init(_hadoopConf);
        _yarn.start();


    }

    private String shellScriptPath = "";

    //public static final String SCRIPT_PATH = "ExecScript";
    private boolean keepContainers = false;
    private int amMemory = 10;
    private int amVCores = 1;
    private int containerMemory = 10;
    private int containerVirtualCores = 1;
    private int numContainers = 1;
    private int shellCmdPriority = 0;
    private String appName = "superorange";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "default";

    private Map<String, String> shellEnv = new HashMap<String, String>();


  //  private static String inputIP = "172.17.1.11";

    public static void main(String args[]) {

        //inputIP = args[0];
        MyClient CLIENT = new MyClient("org.nchc.yarnapp.MyApplicationMaster");
        try {
            CLIENT.launchApp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void launchApp() throws Exception {

        //parse variable from command

        /*
        appName = cliParser.getOptionValue("appname", "DistributedShell");
        appMasterJar = cliParser.getOptionValue("jar");


        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
        if (amMemory < 0) {
         throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
         + " Specified memory=" + amMemory);
        }

        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
        if (amVCores < 0) {
          throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
        }
        shellCmdPriority = Integer.parseInt(cliParser.getOptionValue("shell_cmd_priority", "0"));

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));


        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }
 */



        //Once a client is set up, the client needs to create an application, and get its application id.
        YarnClientApplication app = _yarn.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse(); //for getting Maxmem
        //_appId = appResponse.getApplicationId();

        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        _appId = appContext.getApplicationId();

        System.out.println("AppId from Context = " + _appId);  //same as get from Response


        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();


        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        String appMasterJar = findContainingJar(MyApplicationMaster.class);
        System.out.println("jar name: "+appMasterJar);
        FileSystem fs = FileSystem.get(_hadoopConf);
        Path src = new Path(appMasterJar);
        System.out.println("fshome : "+fs.getHomeDirectory());
        System.out.println("path: "+fs.getHomeDirectory()+"yarnapp/"+_appId.toString() + Path.SEPARATOR + "AppMaster.jar");
        Path dst = new Path(fs.getHomeDirectory(),"yarnapp/"+_appId.toString() + Path.SEPARATOR + "AppMaster.jar");
        fs.copyFromLocalFile(false, true, src, dst);
        localResources.put("AppMaster.jar", Util.newYarnAppResource(fs, dst));


    //    addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),  localResources, null);

        // Set the log4j properties if needed

       /* if (!log4jPropFile.isEmpty()) {
            addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
                    localResources, null);
        }*/

        // The shell script has to be made available on the final container(s)
        // where it will be executed.
        // To do this, we need to first copy into the filesystem that is visible
        // to the yarn framework.
        // We do not need to set this as a local resource for the application
        // master as the application master does not need it.
        String hdfsShellScriptLocation = "";
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        /*if (!shellScriptPath.isEmpty()) {
            Path shellSrc = new Path(shellScriptPath);
            String shellPathSuffix =
                    appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
            Path shellDst =
                    new Path(fs.getHomeDirectory(), shellPathSuffix);
            fs.copyFromLocalFile(false, true, shellSrc, shellDst);
            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
        }

        if (!shellCommand.isEmpty()) {
            addToLocalResources(fs, null, shellCommandPath, appId.toString(),
                    localResources, shellCommand);
        }

        if (shellArgs.length > 0) {
            addToLocalResources(fs, null, shellArgsPath, appId.toString(),
                    localResources, StringUtils.join(shellArgs, " "));
        }*/

        // Set the env variables to be setup in the env where the application master will be run
        //LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        // put location of shell script into env
        // using the env info, the application master will create the correct local resource for the
        // eventual containers that will be launched to execute the shell scripts
        env.put("DISTRIBUTEDSHELLSCRIPTLOCATION", hdfsShellScriptLocation);
        env.put("DISTRIBUTEDSHELLSCRIPTTIMESTAMP", Long.toString(hdfsShellScriptTimestamp));
        env.put("DISTRIBUTEDSHELLSCRIPTLEN", Long.toString(hdfsShellScriptLen));

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : _hadoopConf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./log4j.properties");
        env.put("CLASSPATH", classPathEnv.toString());

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        //LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(_appMasterMainClass+" "/*+inputIP+" "*/);
        // Set params for Application Master
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));

        System.out.println("java home= "+ApplicationConstants.Environment.JAVA_HOME);

        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        /*if (debugFlag) {
            vargs.add("--debug");
        }*/

        //vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        //vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        System.out.println("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);

        System.out.println("capability = " + capability); //memory 10 vcore 1
        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = _hadoopConf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.

            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);


        // Set the priority for the application master
        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        /*By dong's book
        SubmitApplicationRequest request = new Records.newRecord(SubmitApplicationRequest.class);
        request.setApplicationSubmissionContext(appContext);
        _yarn.submitApplication(request);*/


        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        _yarn.submitApplication(appContext);



    }

    private ApplicationReport getReport() throws IOException, YarnException {

        // Get application report for the appId we are interested in
        return _yarn.getApplicationReport(_appId);

    }

    private void killJob() throws IOException, YarnException {
        _yarn.killApplication(_appId);

    }
    public static String findContainingJar(Class<?> my_class) throws IOException {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        for(Enumeration<java.net.URL> itr = loader.getResources(class_file);
            itr.hasMoreElements();) {
            java.net.URL url = itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }

        throw new IOException("Fail to locat a JAR for class: "+my_class.getName());
    }
  /*  private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId,
                                     Map<String, LocalResource> localResources, String resources) throws IOException
    {

        System.out.println("src: "+fileSrcPath);
        System.out.println("dst: "+fileDstPath);
        System.out.println("appId: "+appId);
        System.out.println("resources: "+resources);
        String suffix =  appName + "/" + appId + "/" + fileDstPath;
        System.out.println("suffix: "+suffix);
        Path dst = new Path(fs.getHomeDirectory(), suffix);

        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }*/
}
