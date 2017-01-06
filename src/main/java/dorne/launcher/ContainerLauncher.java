package dorne.launcher;

import dorne.DockerAppMaster;
import dorne.NMCallbackHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

/**
 * Created by 1403035 on 2016/6/29.
 */
public abstract class ContainerLauncher implements Runnable{
    private static final Log LOG = LogFactory.getLog(ContainerLauncher.class);

    // Allocated container
    Container container;

    NMCallbackHandler containerListener;

    DockerAppMaster dockerAppMaster;

    public ContainerLauncher(){}

    public ContainerLauncher(Container lcontainer,  DockerAppMaster dockerAppMaster) {
        this.container = lcontainer;
        this.containerListener = dockerAppMaster.getContainerListener();
        this.dockerAppMaster = dockerAppMaster;
    }

    @Override
    public void run() {
        LOG.info("Setting up container launch container for containerid=" + container.getId());
        process(getContainerCtx());
    }

    // Type 1: shell cmd : CLILauncher
    // Type 2" execute docker-java api in AM : APILauncher
    public abstract void process(ContainerLaunchContext ctx);

    public ContainerLaunchContext getContainerCtx() {
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        return ctx;
    }
}
