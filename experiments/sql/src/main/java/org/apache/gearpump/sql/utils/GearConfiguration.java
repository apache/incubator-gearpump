package org.apache.gearpump.sql.utils;

import com.typesafe.config.Config;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;


public class GearConfiguration {

    private Config akkaConf;
    private ClientContext context;
    public static JavaStreamApp app;

    public void defaultConfiguration() {
        setAkkaConf(ClusterConfig.defaultConfig());
        setContext(ClientContext.apply(akkaConf));
    }

    public void ConfigJavaStreamApp() {
        app = new JavaStreamApp("JavaDSL", context, UserConfig.empty());
    }

    public void setAkkaConf(Config akkaConf) {
        this.akkaConf = akkaConf;
    }

    public void setContext(ClientContext context) {
        this.context = context;
    }
}
