package dorne.launcher;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.DockerAppMaster;
import dorne.DorneConst;
import dorne.bean.ServiceBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;

import java.util.*;


/**
 * See https://github.com/docker-java/docker-java/wiki for other example
 */
public class APILauncher extends ContainerLauncher {

    private static final Log LOG = LogFactory.getLog(APILauncher.class);

    // YARN related parameters
    String yarnContainerHost;

    //Docker related parameters
    DockerClient docker;
    String dockerContainerID;
    ServiceBean service;

    String serviceName;

    public APILauncher(Container yarnContainer, String name ,DockerAppMaster dockerAppMaster){
        //TODO: check docker name is already exist
        super(yarnContainer, dockerAppMaster);
        this.yarnContainerHost = yarnContainer.getNodeId().getHost();

        // create docker java client
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://" + yarnContainerHost + ":" + DorneConst.DOREN_DOCKERHOST_PORT)
                .build();
        this.docker = DockerClientBuilder.getInstance(config).build();

        this.serviceName = name;
        this.service = dockerAppMaster.getComposeConfig().get(name);
    }

    @Override
    public void process(ContainerLaunchContext ctx) {
        // Start docker container using docker-java API
        CreateContainerResponse dockerContainer = setupDockerClient(this.service);
        this.dockerContainerID = dockerContainer.getId();
        docker.startContainerCmd(this.dockerContainerID).exec();

        dockerAppMaster.getDockerContainerMap().put(
                this.dockerContainerID, this.yarnContainerHost);

        dockerAppMaster.getServiceContainerMap().put(
                this.serviceName, this.dockerContainerID);


        // Because YARN create processes on nodemanager by shell command eventually,
        // we create a docker attach command to attach a running container.
        List<String> commands = new ArrayList<String>();
        commands.add(attachContainerCmd(this.dockerContainerID));
        ctx.setCommands(commands);
        containerListener.addContainer(container.getId(), container);
        dockerAppMaster.getNMClientAsync().startContainerAsync(container, ctx);
    }

    /*
    * Setup CreateContainerCmd using ServiceBean context
    * */
    private CreateContainerResponse setupDockerClient(ServiceBean service){

        CreateContainerCmd cmd ;

        // setup image
        if(service.getImage().isEmpty() || service.getImage() == null) {
            return null;
        }else{
            cmd = docker.createContainerCmd(this.service.getImage());
        }

        // setup command
        if(service.getCommand() !=null && !service.getCommand().isEmpty() ){
            String cmdString = service.getCommand();
            cmd.withCmd(Arrays.asList(cmdString.split(" ")));
        }

        // setup container memory limit
        if(service.getMemory() != null)
            cmd.withMemory(service.getMemoryInByte());

        // setup container DNS
        if(service.getDns() != null)
            cmd.withDns(service.getDns());

        // setup container Environment variable
        if(service.getEnvironment()!=null)
            cmd.withEnv(service.getEnvironmentList());

        // setup container name
        if(service.getContainer_name() != null && !service.getContainer_name().isEmpty())
            cmd.withName(service.getContainer_name());

        // TODO: setup other docker container properties from ServiceBean

        // Execute created command and return response
        return cmd.exec();
    }

    /**
     * create docker attach CLI command
     * */
    private String attachContainerCmd(String contianerID){
        Vector<CharSequence> vargs = new Vector<>();
        vargs.add("docker");
        vargs.add("attach");
        vargs.add(contianerID);
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        return command.toString();
    }
}
