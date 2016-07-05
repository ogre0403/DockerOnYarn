package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by 1403035 on 2016/6/29.
 */
public class CLILauncher extends ContainerLauncher{

    private static final Log LOG = LogFactory.getLog(CLILauncher.class);

    public CLILauncher(Container lcontainer,  DockerAppMaster dockerAppMaster){
        super(lcontainer,dockerAppMaster);
    }

    @Override
    public void process(ContainerLaunchContext ctx) {
        ctx.setLocalResources(buildContainerLocalResource());
        List<String> commands = new ArrayList<String>();
        commands.add(buildContainerCmd());
        ctx.setCommands(commands);
        containerListener.addContainer(container.getId(), container);
        dockerAppMaster.getNMClientAsync().startContainerAsync(container, ctx);
    }

    private  String buildContainerCmd(){
        String[] cmdArgs = dockerAppMaster.containerCmdArgs
                .split(DorneConst.DOREN_ARGS_SEPERATOR);

        // Set the necessary command to execute on the allocated container
        Vector<CharSequence> vargs = new Vector<>();
        vargs.add("bash");
        // different container type results in different ExecScript.sh
        vargs.add(DorneConst.DOREN_LOCALRESOURCE_SCRIPT);

        for(String arg: cmdArgs){
            vargs.add(arg);
        }

        // Add log redirect params
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        // Build final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        return command.toString();
    }
}
