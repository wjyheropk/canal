package com.alibaba.otter.canal.instance.core;

import java.io.IOException;
import java.util.List;

import com.alibaba.otter.canal.protocol.Message;

import lombok.AllArgsConstructor;
import lombok.Data;

public interface CanalMQProducer {

    /**
     * Init producer.
     *
     * @param mqProperties MQ config
     */
    void init(MQProperties mqProperties);

    /**
     * Send canal message to related topic
     *
     * @param canalDestination canal mq destination
     * @param message          canal message
     *
     * @throws IOException
     */
    void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) throws IOException;

    /**
     * send directly
     *
     * @param topic     kafka topic
     * @param timestamp kafka timestamp for kafka stream
     * @param keyValues key-value list
     */
    void send(String topic, Long timestamp, List<KeyValue> keyValues);

    /**
     * Stop MQ producer service
     */
    void stop();

    interface Callback {

        void commit();

        void rollback();
    }

    @AllArgsConstructor
    @Data
    class KeyValue {
        private String key;
        private String value;
    }

}
