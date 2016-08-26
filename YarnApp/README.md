 ### 
 
 ```
 $ mvn clean dependency:copy-dependencies package
 copy all jars in target/dependceny into $DORNE_HOME/lib on each host
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar  dorne.Client \
   --jar dorne-1.0-SNAPSHOT.jar \
   <--yaml storm.yml>
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.thrift.ThriftClient \ 
   --server <host> --port <port>
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.thrift.ThriftClient 
   --server <host> --port <port> \ 
   --operation <scale|show|remove|shutdown> [--service <service>=<number>]
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.thrift.ThriftClient --server slave2 --port 44061 --operation shutdown
 ```
 
 ----------
 Cluster mode
 Client mode
 
 --------
 Volume: should have mountable distributed FS
 
 
  docker start -ai 977