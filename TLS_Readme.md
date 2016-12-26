# add following properties in yarn-site.xml

```
<property>
    <name>yarn.timeline-service.hostname</name>
    <value>master</value>
</property>

<property>
    <name>yarn.timeline-service.enabled</name>
    <value>true</value>
</property>

<property>
    <description>per application detail information will be deleted in this interval, 
    default is one week</description>
    <name>yarn.timeline-service.ttl-ms</name>
    <value>600000</value>
</property>

<property>
    <description>Enable generic application information</description>
    <name>yarn.timeline-service.generic-application-history.enabled</name>
    <value>true</value>
</property>

<property>
    <description>Store location on HDFS</description>
    <name>yarn.timeline-service.generic-application-history.fs-history-store.uri</name>
    <value>/tmp/timeline</value>
</property>

<property>
    <name>yarn.timeline-service.generic-application-history.store-class</name>
    <value>org.apache.hadoop.yarn.server.applicationhistoryservice.FileSystemApplicationHistoryStore</value>
</property>
```

# add following configuration in mapred-site.xml
```
    <property>
      <description>let MR job records information using timeline server</description>
      <name>mapreduce.job.emit-timeline-data</name>
      <value>true</value>
   </property>
```