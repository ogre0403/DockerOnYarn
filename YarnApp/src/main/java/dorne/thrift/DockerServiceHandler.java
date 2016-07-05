package dorne.thrift;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.DockerAppMaster;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by 1403035 on 2016/6/4.
 */
public class DockerServiceHandler implements DockerService.Iface {

    DockerAppMaster dockerAppMaster;

    public DockerServiceHandler(DockerAppMaster am){
        this.dockerAppMaster = am;
    }

    @Override
    public void addContainer(int n) throws TException {

        try {

            for(int i=0;i<n;i++) {
                AMRMClient.ContainerRequest request = setupContainerAskForRM();
                dockerAppMaster.getRMClientAsync().addContainerRequest(request);
                dockerAppMaster.getRequestList().put(request);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delContainer(int n) throws TException {
        LinkedBlockingQueue<Pair<String, String>> queue = dockerAppMaster.getContainerList();
        int queue_size = queue.size();
        int size = queue_size < n ? queue_size : n;

        for(int i = 0;i < size; i++){
            Pair<String, String> element = queue.poll();

            DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                    .withDockerHost("tcp://" + element.getValue() +":2375")
                    .build();
            DockerClient docker = DockerClientBuilder.getInstance(config).build();
            docker.stopContainerCmd(element.getKey()).exec();
            docker.removeContainerCmd(element.getKey()).exec();

        }
    }

    @Override
    public Map<String, String> showContainer() throws TException {
        LinkedBlockingQueue<Pair<String, String>> queue = dockerAppMaster.getContainerList();
        HashMap<String, String> map = new HashMap<>();
        for(Pair<String,String> p : queue){
            map.put(p.getKey(),p.getValue());
        }
        return map;
    }

    private AMRMClient.ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        capability.setVirtualCores(1);

        AMRMClient.ContainerRequest request =
                new AMRMClient.ContainerRequest(capability, null, null, pri);
        return request;
    }

}
