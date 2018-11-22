package com.alibaba.otter.canal.instance.core;

import java.util.Properties;

/**
 * 配置项
 *
 * @author wangjiayin
 * @since 2018/11/19
 */
public class CanalConfig {

    private static CanalConfig CONF = new CanalConfig();

    private Properties canalProperties;
    private MQProperties mqProperties;

    private CanalConfig() {
    }

    public static CanalConfig getInstance() {
        return CONF;
    }

    public synchronized void setCanalProperties(Properties properties) {
        if (this.canalProperties == null) {
            this.canalProperties = properties;
        }
    }

    public synchronized void setMqProperties(MQProperties properties) {
        if (this.mqProperties == null) {
            this.mqProperties = properties;
        }
    }

    public Properties getCanalProperties() {
        return canalProperties;
    }

    public MQProperties getMqProperties() {
        return mqProperties;
    }
}
