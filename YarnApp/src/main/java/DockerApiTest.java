import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

/**
 * Created by 1403035 on 2016/6/23.
 */
public class DockerApiTest {
    public static void main(String[] args) {
        DockerClientConfig config = DockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://192.168.33.20:2375")
                .build();
        DockerClient docker = DockerClientBuilder.getInstance(config).build();
        docker.createContainerCmd("nginx").exec();

//        DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://192.168.33.20:2375").build();
//        CreateContainerResponse container = dockerClient.createContainerCmd("nginx")
//                .withHostName("master")
//                .withName("master")
//                .withTty(true)
//                .withStdinOpen(true)
//                .exec();

    }
}
