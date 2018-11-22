package com.alibaba.otter.canal.kafka;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.instance.core.CanalMQProducer;
import com.alibaba.otter.canal.instance.core.MQProperties;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage1;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    // 用于扁平message的数据投递
    private Producer<String, String> producer2;

    @Override
    public void init(MQProperties kafkaProperties) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaProperties.getServers());
        properties.put("acks", "1");
        properties.put("retries", kafkaProperties.getRetries());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        producer2 = new KafkaProducer<>(properties);

        // producer.initTransactions();
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer2 != null) {
                producer2.close();
            }
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    @Override
    public void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) {

        // producer.beginTransaction();
        // 发送扁平数据json
        List<FlatMessage1> flatMessages = null;
        try {
            flatMessages = this.messageConverter(message, canalDestination);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        if (flatMessages != null) {
            for (FlatMessage1 flatMessage : flatMessages) {
                try {
                    logger.info("begin to sent flat message to kafka, topic={}", flatMessage.getTable());
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(flatMessage.getTable(), null,
                                    flatMessage.getEs(), flatMessage.getKey(), flatMessage.getValue());
                    producer2.send(record).get();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    // producer.abortTransaction();
                    callback.rollback();
                    return;
                }
            }
        }

        // producer.commitTransaction();
        callback.commit();

    }

    @Override
    public void send(String topic, Long timestamp, List<KeyValue> keyValues) {
        logger.info("topic={}, data size={}", topic, keyValues.size());
        for (KeyValue data : keyValues) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, null, timestamp, data.getKey(), data.getValue());
            try {
                producer2.send(record);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException("fail to send to kafka");
            }
        }
    }

    private List<FlatMessage1> messageConverter(Message message,
                                                MQProperties.CanalDestination canalDestination)
            throws InvalidProtocolBufferException {

        List<FlatMessage1> result = new LinkedList<>();
        List<CanalEntry.Entry> entrys = null;
        if (message.isRaw()) {
            List<ByteString> rawEntries = message.getRawEntries();
            entrys = new ArrayList<>(rawEntries.size());
            for (ByteString byteString : rawEntries) {
                CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(byteString);
                entrys.add(entry);
            }
        } else {
            entrys = message.getEntries();
        }

        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            CanalEntry.Header header = entry.getHeader();
            Long timestamp = header.getExecuteTime();
            String tableName = header.getSchemaName() + "." + header.getTableName();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<FlatMessage1> batch = rowChange.getRowDatasList().stream()
                    .map(rowData ->
                            convertRowData(tableName, timestamp, rowData, rowChange.getEventType(), canalDestination))
                    .collect(Collectors.toList());
            result.addAll(batch);
        }

        return result;
    }

    private FlatMessage1 convertRowData(String tableName, long timestamp,
                                        CanalEntry.RowData rowData,
                                        CanalEntry.EventType eventType,
                                        MQProperties.CanalDestination canalDestination) {
        String uniqueCol = canalDestination.getTableKey().get(tableName);
        Map<String, String> rowDataMap;
        // 判断更新类型
        if (eventType == CanalEntry.EventType.DELETE) {
            rowDataMap = rowData.getBeforeColumnsList().stream()
                    .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
        } else {
            rowDataMap =
                    rowData.getAfterColumnsList().stream()
                            .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
        }
        String kafkaKey = rowDataMap.get(uniqueCol);
        // 删除不需要的列
        rowDataMap.remove(uniqueCol);
        List<String> ignoreList = canalDestination.getIgnoreColumns().get(tableName);
        if (ignoreList != null) {
            ignoreList.forEach(rowDataMap::remove);
        }
        if (eventType == CanalEntry.EventType.DELETE) {
            return new FlatMessage1(tableName, timestamp, kafkaKey, null);
        } else if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
            return new FlatMessage1(tableName, timestamp, kafkaKey, JsonUtils.marshalToString(rowDataMap));
        }
        return null;
    }

}
