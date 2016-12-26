package dorne.launcher;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import dorne.DockerAppMaster;
import dorne.DorneConst;
import dorne.bean.ServiceBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
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
        CreateContainerResponse dockerContainer = this.service.createContainer(this.docker);
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

    private LocalResource cretaeLocalResource() throws IOException {
        FileSystem fs = FileSystem.get(null);
        //TODO: fixed file path
        String suffix = "docker";
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE,
                        LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(),
                        scFileStatus.getModificationTime());
        return scRsrc;
    }
}
