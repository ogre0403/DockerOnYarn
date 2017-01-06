package dorne.launcher;

import dorne.DockerAppMaster;
import dorne.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Created by ogre0403 on 2017/1/3.
 */
public class SlaveRunnerLauncher extends ContainerLauncher {


    private static final Log LOG = LogFactory.getLog(SlaveRunnerLauncher.class);

    String serviceName;

    public SlaveRunnerLauncher(Container yarnContainer, String name , DockerAppMaster dockerAppMaster){
        super(yarnContainer, dockerAppMaster);
        this.serviceName = name;
    }

    @Override
    public void process(ContainerLaunchContext ctx) {

        containerListener.addContainer(container.getId(), container);
        dockerAppMaster.getNMClientAsync().startContainerAsync(container, ctx);

    }

    @Override
    public ContainerLaunchContext getContainerCtx() {
        // To some configuration...

        ContainerLaunchContext ctx = super.getContainerCtx();
        List<String> commands = new ArrayList<String>();
        commands.add(buildCommand());
        ctx.setCommands(commands);

        try {
            ctx.setLocalResources(getSlaveRunnerLocalResource());

            Map<String, String> env = new HashMap<String, String>();
            ctx.setEnvironment(Util.buildEnv(env));

        } catch (IOException e) {
            e.printStackTrace();
        }


        return ctx;

    }

    private Map<String, LocalResource> getSlaveRunnerLocalResource() throws IOException {
        LOG.info("add App Master jar in HDFS to local resource");

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        FileSystem fs = FileSystem.get(new YarnConfiguration());

        //TODO: not hard code file path in HDFS
        String suffix = "AppMaster.jar";
        Path dst = new Path(fs.getHomeDirectory(), suffix);

        LocalResource flinkJar = Records.newRecord(LocalResource.class);
        Util.registerLocalResource(fs, dst,flinkJar);
        localResources.put("slave.jar", flinkJar);

        return localResources;
    }



    /**
     * Build AM launch command
     * */
    private String buildCommand(){

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set class name
        vargs.add(DorneSlaveRunner.class.getName());
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppSlave.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppSlave.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        LOG.info("Completed setting up app master command " + command.toString());
        return command.toString();
    }

}
