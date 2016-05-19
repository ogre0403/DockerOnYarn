 ### 
 aaa
 
 ```
 $ hadoop jar dorne-1.0-SNAPSHOT.jar dorne.Client \
   --jar dorne-1.0-SNAPSHOT.jar \
   --docker_image ubuntu \ 
   --num_docker_containers 3 \
   --docker_container_cmd ls
 ```