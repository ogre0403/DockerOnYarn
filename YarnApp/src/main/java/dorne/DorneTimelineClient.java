package dorne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by 1403035 on 2016/7/19.
 */
public class DorneTimelineClient extends TLSClient {

    private static final Log LOG = LogFactory.getLog(DorneTimelineClient.class);

    DorneTimelineClient(Configuration conf) throws IOException {
        super(conf);
    }

    public void publish1() throws IOException, InterruptedException {
        publishGenericEvent("","","","",null);
    }
}
