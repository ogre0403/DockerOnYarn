package dorne.timeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map;

public class TLSClient {

    private static final Log LOG = LogFactory.getLog(TLSClient.class);

    TimelineClient tlsClient;
    UserGroupInformation appSubmitterUgi;

    TLSClient(Configuration conf) throws IOException {

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }

        String appSubmitterUserName =
                System.getenv(ApplicationConstants.Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);


        // Creating the Timeline Client
        tlsClient = TimelineClient.createTimelineClient();
        tlsClient.init(conf);
        tlsClient.start();
    }

    protected void close() throws IOException {
        this.tlsClient.close();
    }

    protected void publishGenericEvent(
            String domain,
            String entityType, String entityID,
            String eventType,
            Map<String, String> extraInfo) throws IOException, InterruptedException {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(entityID);
        entity.setEntityType(entityType);
        entity.setDomainId(domain);
        entity.addPrimaryFilter("user", appSubmitterUgi.getShortUserName());

        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(eventType);

        if(extraInfo != null) {
            Iterator iter = extraInfo.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry) iter.next();
                event.addEventInfo(entry.getKey(), entry.getValue());
            }
        }
        entity.addEvent(event);

        appSubmitterUgi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
            @Override
            public TimelinePutResponse run() throws Exception {
                return tlsClient.putEntities(entity);
            }
        });
    }
}
