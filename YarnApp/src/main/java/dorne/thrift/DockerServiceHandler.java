package dorne.thrift;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.DockerAppMaster;
import dorne.DorneConst;
import dorne.Util;
import dorne.bean.HostInfo;
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

public class DockerServiceHandler implements DockerService.Iface {
    private static final Log LOG = LogFactory.getLog(DockerServiceHandler.class);

    DockerAppMaster dockerAppMaster;

    public DockerServiceHandler(DockerAppMaster am){
        this.dockerAppMaster = am;
    }

    /**
     * Add n services which identifies by name
     */
    @Override
    public void scaleService(String name, int n) throws TException {
        List<String> serviceNames = dockerAppMaster.getSortedServiceName();
        Map<String, ServiceBean> beans = dockerAppMaster.getComposeConfig();
        ServiceBean cloneBean;
        try {
            for (int i = 0; i < n; i++) {
                cloneBean = (ServiceBean) beans.get(name).clone();
                long epoch = System.currentTimeMillis() / 1000;

                // avoid DNS naming conflict, append timestamp to original service name
                String extendName = dockerAppMaster.getAppid() + "-" +name + "-" + epoch;
                resloveNamingConflict(cloneBean, extendName);
                Util.ReplaceServiceNameVariable(cloneBean, (ConcurrentHashMap)beans);

                serviceNames.add(extendName);
                beans.put(extendName, cloneBean);

                AMRMClient.ContainerRequest request = setupContainerAskForRM();
                dockerAppMaster.getRMClientAsync().addContainerRequest(request);
                dockerAppMaster.getRequestList().put(request);

                dockerAppMaster.getNumRequestedContainers().incrementAndGet();

                // Make sure extendName is different
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Avoid naming conflict, remove container name inside clone service bean,
     * and replace SERVICE_NAME with name appended with timestamp
     */
    private void resloveNamingConflict(ServiceBean bean, String extendName){
        // remove container_name
        if(bean.getContainer_name() != null)
            bean.setContainer_name("");

        // replace environment SERVICE_NAME=<extendName>
        bean.getEnvironment().put("SERVICE_NAME", extendName);
    }

    /**
     * Remove a service by name
     */
    @Override
    public void removeService(String name) throws TException {
        Map<String, String> nameDockerIDMap = dockerAppMaster.getServiceContainerMap();
        Map<String, String> dockerHostMap = dockerAppMaster.getDockerContainerMap();

        if (nameDockerIDMap.containsKey(name)){
            String dockerId = nameDockerIDMap.remove(name);

            if(dockerHostMap.containsKey(dockerId)){
                String host = dockerHostMap.remove(dockerId);

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

    /*
     * Return a list of all running docker container information.
     * Each String format ServiceName/ContainerName@HOST:containerIP
     */
    @Override
    public List<String> showServices() throws TException {
        // use to iterate service name
        Map<String, String> nameDockerIDMap = dockerAppMaster.getServiceContainerMap();

        // use to find container location by docker ID
        Map<String, String> dockerHostMap = dockerAppMaster.getDockerContainerMap();

        List<String> infoList = new LinkedList<>();
        for(Map.Entry<String, String> e: nameDockerIDMap.entrySet()){
            String name = e.getKey();
            String dockerID = e.getValue();
            String host = dockerHostMap.get(dockerID);
            HostInfo ipAndName = getDockerIPAddressAndName(host, dockerID);
            infoList.add(name+ipAndName.getHostname()+"@"+host+":"+ipAndName.getIp());
        }
        return infoList;
    }

    @Override
    public int getContainerNum() throws TException {
        return dockerAppMaster.getNumAllocatedContainers().get();
    }

    /**
     * Use Docker inspect java api to find container name and internal IP.
     * First element of return array is container.
     * Second element of return array is internal IP.
     */
    private HostInfo getDockerIPAddressAndName(String host, String conID)  {
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

        HostInfo hi = new HostInfo();
        hi.setHostname(r.getName());
        hi.setIp(r.getNetworkSettings().getIpAddress());
        return hi;
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
