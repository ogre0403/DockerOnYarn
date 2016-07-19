package dorne.launcher;

import dorne.DockerAppMaster;
import dorne.DorneConst;
import dorne.NMCallbackHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        process(ctx);
    }

    // TODO: Docker API approach
    // 0. shell cmd : CLILauncher
    // 1. execute docker-java api in AM : APILauncher
    public abstract void process(ContainerLaunchContext ctx);

//    public abstract String buildContainerCmd();

}
