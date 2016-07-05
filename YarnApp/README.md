 ### 
 
 ```
 $ mvn clean dependency:copy-dependencies package
 copy all jars in target/dependceny into $DORNE_HOME/lib on each host
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar  dorne.Client \
   --jar dorne-1.0-SNAPSHOT.jar \
   --docker_container_num 5 \
   --appname num_5
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.Client \
   --jar dorne-1.0-SNAPSHOT.jar \
   --docker_service <ping|nginx> \
   --docker_service_args 20#www.google.com  
   --docker_container_num 3 
 ```
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.thrift.ThriftClient \ 
   <AM_HOST> <AM_PORT> <NUM_CONTAINER>
 ```
 