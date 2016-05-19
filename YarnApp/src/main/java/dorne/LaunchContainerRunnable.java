package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

import java.util.*;

/**
 * Created by 1403035 on 2016/5/17.
 */
public class LaunchContainerRunnable implements Runnable {

    private static final Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);

    // Allocated container
    Container container;

    NMCallbackHandler containerListener;

    DockerAppMaster dockerAppMaster;

    public LaunchContainerRunnable(
            Container lcontainer,  DockerAppMaster dockerAppMaster) {
        this.container = lcontainer;
        this.containerListener = dockerAppMaster.containerListener;
        this.dockerAppMaster = dockerAppMaster;
    }

    @Override
    public void run() {
        LOG.info("Setting up container launch container for containerid="
                + container.getId());
        ContainerLaunchContext ctx = Records
                .newRecord(ContainerLaunchContext.class);

        // Set the necessary command to execute on the allocated container
        Vector<CharSequence> vargs = new Vector<CharSequence>();

        // set docker run cmd
        vargs.add(DorneConst.DOREN_DOCKER_RUN_CMD);
        // Set executable command
        vargs.add(dockerAppMaster.containerImage);
        // Set args for the shell command if any
        vargs.add(dockerAppMaster.containerCmd);
//        vargs.add("ping -c 20 8.8.8.8");

        //TODO: support docker cmd with args

        // Add log redirect params
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        // Build final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        ctx.setCommands(commands);


        containerListener.addContainer(container.getId(), container);
        dockerAppMaster.getNMClientAsync().startContainerAsync(container, ctx);
    }
}
