package dorne.thrift;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.DockerAppMaster;
import dorne.DorneConst;
import dorne.bean.ServiceBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by 1403035 on 2016/6/4.
 */
public class DockerServiceHandler implements DockerService.Iface {
    private static final Log LOG = LogFactory.getLog(DockerServiceHandler.class);

    DockerAppMaster dockerAppMaster;

    public DockerServiceHandler(DockerAppMaster am){
        this.dockerAppMaster = am;
    }

    @Override
    public void scaleService(String name, int n) throws TException {

    }

    @Override
    public void removeService(String name) throws TException {
        Map<String, String> nameDockerIDMap = dockerAppMaster.getServiceContainerMap();
        Map<String, String> dockerHostMap = dockerAppMaster.getDockerContainerMap();

        if (nameDockerIDMap.containsKey(name)){
            String dockerId = nameDockerIDMap.get(name);

            if(dockerHostMap.containsKey(dockerId)){
                String host = dockerHostMap.get(dockerId);

                DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                        .withDockerHost("tcp://" + host + ":" + DorneConst.DOREN_DOCKERHOST_PORT)
                        .build();
                DockerClient docker = DockerClientBuilder.getInstance(config).build();
                docker.stopContainerCmd(dockerId).exec();
                docker.removeContainerCmd(dockerId).exec();
                try {
                    docker.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                LOG.warn("Container ID : " + dockerId + " not found !");
            }
        }else{
            LOG.warn("Service Name : " + name + " not found !");
        }
    }

    @Override
    public List<String> showServices() throws TException {
        // use to iterate service name
        Map<String, String> nameDockerIDMap = dockerAppMaster.getServiceContainerMap();

        // use to find container location by docker ID
        Map<String, String> dockerHostMap = dockerAppMaster.getDockerContainerMap();

        // use to get bean by service name
        ConcurrentHashMap<String, ServiceBean> serviceBeans = dockerAppMaster.getComposeConfig();

        List<String> infoList = new LinkedList<>();


        for(Map.Entry<String, String> e: nameDockerIDMap.entrySet()){
            String name = e.getKey();
            String dockerID = e.getValue();
            String host = dockerHostMap.get(dockerID);
            String[] ipAndName = getDockerIPAddressAndName(host, dockerID);
            infoList.add(name+ipAndName[0]+"@"+host+":"+ipAndName[1]);
        }
        return infoList;
    }

    private String[] getDockerIPAddressAndName(String host, String conID)  {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://" + host + ":" + DorneConst.DOREN_DOCKERHOST_PORT)
                .build();
        DockerClient docker = DockerClientBuilder.getInstance(config).build();

        InspectContainerResponse r =  docker.inspectContainerCmd(conID).exec();
        try {
            docker.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String[]{
            r.getName(),
            r.getNetworkSettings().getIpAddress()};
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

    private void addContainer(int n) throws TException {

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

}
