package dorne.bean;

import dorne.DorneConst;

import java.util.List;

public class ServiceBean {

    private static final long b = 1;
    private static final long k = 1024;
    private static final long m = 1024 * 1024;
    private static final long g = 1024 * 1024 * 1024;
    // minimum container memory limit is 4MB
    private static final long minMem = 4 * m;

    private String image;
    private String command;
    private List<String> ports;
    private String container_name;
    private String hostname;
    private List<String> environment;
    private List<String> dns;
    private List<String> depends_on;

    // default container memory is 1024 MB = 1GB
    private String memory = DorneConst.DOREN_YARN_CONTAINER_MEM+"m";

    public ServiceBean(){}

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getMemory() {
        return memory;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public List<String> getPorts() {
        return ports;
    }

    public void setPorts(List<String> ports) {
        this.ports = ports;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public List<String> getEnvironment() {
        return environment;
    }

    public void setEnvironment(List<String> environment) {
        this.environment = environment;
    }

    public List<String> getDns() {
        return dns;
    }

    public void setDns(List<String> dns) {
        this.dns = dns;
    }

    public List<String> getDepends_on() {
        return depends_on;
    }

    public void setDepends_on(List<String> depends_on) {
        this.depends_on = depends_on;
    }

    public String getContainer_name() {
        return container_name;
    }

    public void setContainer_name(String container_name) {
        this.container_name = container_name;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    /*
        * Based on the memory limit unit, return the memory limit in byte.
        * This limit must be larger than 4MB. (Docker constraint)
        * */
    public Long getMemoryInByte() {
        String unit = memory.substring(memory.length() - 1);
        long memInByte ;
        try {
            memInByte = Long.parseLong(memory.substring(0, memory.length() - 1));
        }catch (Exception e){
            return minMem;
        }
        switch (unit){
            case "b":
                memInByte = memInByte * b;
                break;
            case "k":
                memInByte = memInByte * k;
                break;
            case "m":
                memInByte = memInByte * m;
                break;
            case "g":
                memInByte = memInByte * g;
                break;
            default:
                memInByte = memInByte * b;
        }
        return (memInByte < minMem) ? minMem : memInByte;
    }

    /**
     * Iterate the environment list to find environment value by key.
     * Time complexity is O(n).
     */
    public String getEnvByKey(String envKey){
        for(String env: environment){
            String[] kv = env.split("=");
            if (kv[0].equals(envKey))
                return kv[1];
        }
        return null;
    }
}
