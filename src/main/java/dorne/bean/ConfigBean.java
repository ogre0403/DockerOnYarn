package dorne.bean;

import java.util.Map;

/**
 * Created by 1403035 on 2016/7/20.
 */
public class ConfigBean {

    private String version;
    private Map<String, ServiceBean> services;

    public ConfigBean(){}

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, ServiceBean> getServices() {
        return services;
    }

    public void setServices(Map<String, ServiceBean> services) {
        this.services = services;
    }
}
