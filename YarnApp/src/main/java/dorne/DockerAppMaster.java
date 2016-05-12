package dorne;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.LogManager;

import java.io.IOException;

/**
 * Created by 1403035 on 2016/5/12.
 */
public class DockerAppMaster {

    private static final Log LOG = LogFactory.getLog(DockerAppMaster.class);

    // Configuration
    private Configuration conf;

    AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();

    private int numberContainer = 1;

    public DockerAppMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    public static void main(String[] args) {
        boolean result = false;
        try {
            DockerAppMaster appMaster = new DockerAppMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    public boolean init(String[] args) throws ParseException {

        for(String arg:args){
            LOG.info(arg);
        }

        Options opts = new Options();
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_IMAGE, true,
                "Images to be used by docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CMD, true,
                "Command to be executed by docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_MEM, true,
                "Amount of memory in MB to be requested to run docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_CORE, true,
                "Amount of core to be requested to run docker container");
        opts.addOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM, true,
                "No. of containers on which the shell command needs to be executed");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if(!cliParser.hasOption(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM)){
            throw new IllegalArgumentException("No docker number specified");
        }
        numberContainer = Integer.parseInt(
                cliParser.getOptionValue(DorneConst.DOREN_OPTS_DOCKER_CONTAINER_NUM));


        return true;
    }


    public void run() throws IOException, YarnException, InterruptedException {

        rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "");
        int i = 0;
        while(i < numberContainer){
            i++;
            LOG.info( i + "/" + numberContainer);
            Thread.sleep(10000);
        }

    }

    protected boolean finish() throws IOException, YarnException {
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        return true;
    }
}
