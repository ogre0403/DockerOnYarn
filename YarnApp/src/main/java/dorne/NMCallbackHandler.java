package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by 1403035 on 2016/5/13.
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler{

    private static final Log LOG = LogFactory.getLog(NMCallbackHandler.class);

    private final DockerAppMaster dockerAppMaster;

    private ConcurrentMap<ContainerId, Container> containers =
            new ConcurrentHashMap<ContainerId, Container>();

    public NMCallbackHandler(DockerAppMaster applicationMaster) {
        this.dockerAppMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
        containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to start Container " + containerId);
        }
        Container container = containers.get(containerId);
        if (container != null) {
            dockerAppMaster.getNMClientAsync().getContainerStatusAsync(containerId, container.getNodeId());
        }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Container Status: id=" + containerId + ", status=" +
                    containerStatus);
        }
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Succeeded to stop Container " + containerId);
        }
        containers.remove(containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to start Container " + containerId);
        containers.remove(containerId);
        dockerAppMaster.numCompletedContainers.incrementAndGet();
        dockerAppMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to stop Container " + containerId);
        containers.remove(containerId);
    }
}
