package dorne;

import dorne.bean.ServiceBean;
import dorne.launcher.APILauncher;
import dorne.launcher.ContainerLauncher;
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
        // TODO: remove exist docker container
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
                    dockerAppMaster.getNumCompletedContainers().incrementAndGet();
                    dockerAppMaster.getNumFailedContainers().incrementAndGet();
                } else {
                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    dockerAppMaster.getNumAllocatedContainers().decrementAndGet();
                    dockerAppMaster.getNumRequestedContainers().decrementAndGet();
                    // we do not need to release the container as it would be done
                    // by the RM
                }
            } else {
                // nothing to do
                // container completed successfully
                dockerAppMaster.getNumCompletedContainers().incrementAndGet();
                LOG.info("Container completed successfully." + ", containerId="
                        + containerStatus.getContainerId());
            }
        }

        // ask for more containers if any failed
        int askCount = dockerAppMaster.numberContainer - dockerAppMaster.getNumRequestedContainers().get();
        dockerAppMaster.getNumRequestedContainers().addAndGet(askCount);

        if (askCount > 0) {
            for (int i = 0; i < askCount; ++i) {
                AMRMClient.ContainerRequest containerAsk = dockerAppMaster.setupContainerAskForRM();
                dockerAppMaster.getRMClientAsync().addContainerRequest(containerAsk);
            }
        }

        LOG.info("completed container num : " + dockerAppMaster.getNumCompletedContainers().get());
        if (dockerAppMaster.getNumCompletedContainers().get() == dockerAppMaster.numberContainer) {
            dockerAppMaster.setDone(true);
        }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {

        LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());

        for (Container allocatedContainer : allocatedContainers) {
            dockerAppMaster.getNumAllocatedContainers().getAndIncrement();

            LOG.info("Launching shell command on a new container."
                    + ", containerId=" + allocatedContainer.getId()
                    + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                    + ":" + allocatedContainer.getNodeId().getPort()
                    + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                    + ", containerResourceMemory"
                    + allocatedContainer.getResource().getMemory()
                    + ", containerResourceVirtualCores"
                    + allocatedContainer.getResource().getVirtualCores());

            String serviceName = dockerAppMaster.getSortedServiceName().remove(0);
//            ServiceBean service = dockerAppMaster.getComposeConfig().get(name);

            // launch and start the container on a separate thread to keep the main thread unblocked
            ContainerLauncher runnableLaunchContainer =
                    new APILauncher(allocatedContainer, serviceName, dockerAppMaster);
            Thread launchThread = new Thread(runnableLaunchContainer);
            dockerAppMaster.launchThreads.add(launchThread);
            launchThread.start();

            try {
                // Start services in dependency order, and sleep for a while before start next.
                // Latter service will not wait for former be ¡§ready¡¨ before starting
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // About extra containers being allocated.
            // http://mapreduce-user.hadoop.apache.narkive.com/UKO7MiTd/about-extra-containers-being-allocated-in-distributed-shell-example
            // YARN-1902 , SPARK-2687, SLIDER-829, SLIDER-828
            // After get allocated container, container request is remove manually
            if(!dockerAppMaster.requestList.isEmpty()){
                try {
                    dockerAppMaster.getRMClientAsync().removeContainerRequest(dockerAppMaster.requestList.take());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
        int allocated = dockerAppMaster.getNumAllocatedContainers().get();
        int total = dockerAppMaster.numberContainer;
        int complete = dockerAppMaster.getNumCompletedContainers().get();
        float progress =((float) (allocated + complete) )/ ( (float) (total * 2) ) ;
        return progress;
    }

    @Override
    public void onError(Throwable e) {
        dockerAppMaster.setDone(true);
        dockerAppMaster.getRMClientAsync().stop();
    }
}
