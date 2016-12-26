namespace java dorne.thrift  // defines the namespace

typedef i32 int  //typedefs to get convenient names for your types

service DockerService {  // defines the service to add two numbers
           void scaleService(1:string name, 2:int n),
           void removeService(1:string name),
           list<string> showServices(),
           int getContainerNum(),
}