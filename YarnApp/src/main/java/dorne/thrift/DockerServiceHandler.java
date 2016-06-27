package dorne.thrift;

import dorne.DockerAppMaster;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;

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
