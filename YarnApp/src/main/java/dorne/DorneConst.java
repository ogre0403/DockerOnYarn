package dorne;

/**
 * Created by 1403035 on 2016/5/12.
 */
public class DorneConst {

    // application master class name
    static public final String DORNE_APPLICATION_MASTER_CLASS = "dorne.DockerAppMaster";
    static public final String DORNE_APPLICATION_TYPE = "DORNE";

    // client opts name
    static public final String DOREN_OPTS_APPNAME = "appname";
    static public final String DOREN_OPTS_JAR = "jar";

    // AM memory and core
    static public final String DOREN_OPTS_YARN_AM_MEM = "am_memory";
    static public final String DOREN_OPTS_YARN_AM_CORE = "am_vcores";

    // docker container num/mem/core
    static public final String DOREN_OPTS_DOCKER_CONTAINER_NUM = "docker_container_num";
    static public final String DOREN_OPTS_DOCKER_CONTAINER_MEM = "docker_container_memory";
    static public final String DOREN_OPTS_DOCKER_CONTAINER_CORE = "docker_container_vcores";

    // dockerized service name & arguments
    static public final String DOREN_OPTS_DOCKER_SERVICE = "docker_service";
    static public final String DOREN_OPTS_DOCKER_SERVICE_ARGS = "docker_service_args";

    // odckerized service arguments seperated by #
    static public final String DOREN_ARGS_SEPERATOR = "#";

    //
    static public final String DOREN_DEMO_FILE = "Demo.sh";
    static public final String DOREN_DEMO_SCRIPTLOCATION = "DORENDEMOSCRIPTLOCATION";
    static public final String DOREN_DEMO_SCRIPTTIMESTAMP = "DORENDEMOSCRIPTTIMESTAMP";
    static public final String DOREN_DEMO_SCRIPTLEN = "DORENDEMOSCRIPTLEN";

    // script name on each nodemanager
    static public final String DOREN_LOCALRESOURCE_SCRIPT = "ExecScript.sh";
}
