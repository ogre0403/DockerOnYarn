package dorne.bean;

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

    // default container memory is 4GB = 4096 MB
    private String memory = "4g";

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getMemory() {
        return memory;
    }

    public ServiceBean(){}

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
}
