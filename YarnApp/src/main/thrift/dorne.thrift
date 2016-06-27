namespace java dorne.thrift  // defines the namespace

typedef i32 int  //typedefs to get convenient names for your types

service DockerService {  // defines the service to add two numbers
           void addContainer(1:int n), //defines a method
           void delContainer(1:int n), //defines a method
}