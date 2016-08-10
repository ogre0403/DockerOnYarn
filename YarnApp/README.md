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
   <AM_HOST> <AM_PORT> <NUM_CONTAINER>
 ```
 