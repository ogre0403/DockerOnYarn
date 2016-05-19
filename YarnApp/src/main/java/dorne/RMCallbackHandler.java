package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

/**
 * Created by 1403035 on 2016/5/13.
 */
public class RMCallbackHandler  implements AMRMClientAsync.CallbackHandler {

    private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);
    private final DockerAppMaster dockerAppMaster;

    public RMCallbackHandler(DockerAppMaster applicationMaster) {
        this.dockerAppMaster = applicationMaster;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
        LOG.info("Got response from RM for container ask, completedCnt="
                + completedContainers.size());
        for (ContainerStatus containerStatus : completedContainers) {
            LOG.info("Got container status for containerID="
                    + containerStatus.getContainerId() + ", state="
                    + containerStatus.getState() + ", exitStatus="
                    + containerStatus.getExitStatus() + ", diagnostics="
                    + containerStatus.getDiagnostics());

            // non complete containers should not be here
            assert (containerStatus.getState() == ContainerState.COMPLETE);

            // increment counters for completed/failed containers
            int exitStatus = containerStatus.getExitStatus();
            if (0 != exitStatus) {
                // container failed
                if (ContainerExitStatus.ABORTED != exitStatus) {
                    // shell script failed
                    // counts as completed
                    dockerAppMaster.numCompletedContainers.incrementAndGet();
                    dockerAppMaster.numFailedContainers.incrementAndGet();
                } else {
                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    dockerAppMaster.numAllocatedContainers.decrementAndGet();
                    dockerAppMaster.numRequestedContainers.decrementAndGet();
                    // we do not need to release the container as it would be done
                    // by the RM
                }
            } else {
                // nothing to do
                // container completed successfully
                dockerAppMaster.numCompletedContainers.incrementAndGet();
                LOG.info("Container completed successfully." + ", containerId="
                        + containerStatus.getContainerId());
            }
        }

        // ask for more containers if any failed
        int askCount = dockerAppMaster.numberContainer - dockerAppMaster.numRequestedContainers.get();
        dockerAppMaster.numRequestedContainers.addAndGet(askCount);

        if (askCount > 0) {
            for (int i = 0; i < askCount; ++i) {
                AMRMClient.ContainerRequest containerAsk = dockerAppMaster.setupContainerAskForRM();
                dockerAppMaster.getRMClientAsync().addContainerRequest(containerAsk);
            }
        }

        LOG.info("completed container num : " + dockerAppMaster.numCompletedContainers.get());
        if (dockerAppMaster.numCompletedContainers.get() == dockerAppMaster.numberContainer) {
            dockerAppMaster.setDone(true);
        }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {

        LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());

        for (Container allocatedContainer : allocatedContainers) {

            // TODO: total allocated container may be larger than request
            // avoid invoke too more container than required
            int allocated_num = dockerAppMaster.numAllocatedContainers.getAndIncrement();
            if(allocated_num >= dockerAppMaster.numberContainer) {
                // TODO: do a very dummy task

                break;
            }

            LOG.info("Launching shell command on a new container."
                    + ", containerId=" + allocatedContainer.getId()
                    + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                    + ":" + allocatedContainer.getNodeId().getPort()
                    + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                    + ", containerResourceMemory"
                    + allocatedContainer.getResource().getMemory()
                    + ", containerResourceVirtualCores"
                    + allocatedContainer.getResource().getVirtualCores());

            LaunchContainerRunnable runnableLaunchContainer =
                    new LaunchContainerRunnable(allocatedContainer, dockerAppMaster);
            Thread launchThread = new Thread(runnableLaunchContainer);

            // launch and start the container on a separate thread to keep
            // the main thread unblocked
            // as all containers may not be allocated at one go.
            dockerAppMaster.launchThreads.add(launchThread);
            launchThread.start();

        }
    }

    @Override
    public void onShutdownRequest() {
        dockerAppMaster.setDone(true);
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
        // set progress to deliver to RM on next heartbeat
        float progress = (float) dockerAppMaster.numCompletedContainers.get()
                / dockerAppMaster.numberContainer;
        return progress;
    }

    @Override
    public void onError(Throwable e) {
        dockerAppMaster.setDone(true);
        dockerAppMaster.getRMClientAsync().stop();
    }
}