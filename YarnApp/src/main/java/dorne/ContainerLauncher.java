package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
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

    Map<String, String> envs;

    public ContainerLauncher(){}

    public ContainerLauncher(Container lcontainer,  DockerAppMaster dockerAppMaster) {
        this.container = lcontainer;
        this.containerListener = dockerAppMaster.containerListener;
        this.dockerAppMaster = dockerAppMaster;
        this.envs = dockerAppMaster.envs;
    }

    @Override
    public void run() {
        LOG.info("Setting up container launch container for containerid=" + container.getId());
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        process(ctx);
    }

    protected Map<String, LocalResource> buildContainerLocalResource(){
        // Set the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        Path renamedScriptPath = new Path(envs.get(DorneConst.DOREN_DEMO_SCRIPTLOCATION));
        LocalResource shellRsrc = Records.newRecord(LocalResource.class);
        shellRsrc.setType(LocalResourceType.FILE);
        shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        try {
            shellRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
                    renamedScriptPath.toString())));
        } catch (URISyntaxException e) {
            LOG.error("Error when trying to use shell script path specified"
                    + " in env, path=" + renamedScriptPath, e);
            dockerAppMaster.numCompletedContainers.incrementAndGet();
            dockerAppMaster.numFailedContainers.incrementAndGet();
            return null;
        }
        shellRsrc.setTimestamp(Long.valueOf(envs.get(DorneConst.DOREN_DEMO_SCRIPTTIMESTAMP)));
        shellRsrc.setSize(Long.valueOf(envs.get(DorneConst.DOREN_DEMO_SCRIPTLEN)));
        localResources.put(DorneConst.DOREN_LOCALRESOURCE_SCRIPT, shellRsrc);
        return localResources;
    }

    // TODO: Docker API approach
    // 0. shell cmd : CLILauncher
    // 1. execute docker-java api in AM : APILauncher
    public abstract void process(ContainerLaunchContext ctx);

}
