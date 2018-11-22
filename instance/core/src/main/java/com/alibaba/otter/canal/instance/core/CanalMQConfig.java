package com.alibaba.otter.canal.instance.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CanalMQConfig {

    private String topic;
    private Integer partition;
    private Integer partitionsNum;
    private String partitionHash;
    private String tableKeysStr;
    private String tableIgnoreColumnsStr;

    private volatile Map<String, String> partitionHashProperties;

    // 数据库表名-主键
    private volatile Map<String, String> tableKey;
    // 数据库表名-忽略的字段
    private volatile Map<String, List<String>> ignoreColumns;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Integer getPartitionsNum() {
        return partitionsNum;
    }

    public void setPartitionsNum(Integer partitionsNum) {
        this.partitionsNum = partitionsNum;
    }

    public String getPartitionHash() {
        return partitionHash;
    }

    public void setPartitionHash(String partitionHash) {
        this.partitionHash = partitionHash;
    }

    public String getTableKeysStr() {
        return tableKeysStr;
    }

    public void setTableKeysStr(String tableKeysStr) {
        this.tableKeysStr = tableKeysStr;
    }

    public String getTableIgnoreColumnsStr() {
        return tableIgnoreColumnsStr;
    }

    public void setTableIgnoreColumnsStr(String tableIgnoreColumnsStr) {
        this.tableIgnoreColumnsStr = tableIgnoreColumnsStr;
    }

    public Map<String, String> getPartitionHashProperties() {
        if (partitionHashProperties == null) {
            synchronized(CanalMQConfig.class) {
                if (partitionHashProperties == null) {
                    if (partitionHash != null) {
                        partitionHashProperties = new LinkedHashMap<>();
                        String[] items = partitionHash.split(",");
                        for (String item : items) {
                            int i = item.indexOf(":");
                            if (i > -1) {
                                String dbTable = item.substring(0, i).trim();
                                String pk = item.substring(i + 1).trim();
                                partitionHashProperties.put(dbTable, pk);
                            }
                        }
                    }
                }
            }
        }
        return partitionHashProperties;
    }

    public Map<String, String> getTableKeys() {
        if (tableKey == null) {
            synchronized(CanalMQConfig.class) {
                if (tableKey == null) {
                    tableKey = new HashMap<>();
                    Arrays.stream(tableKeysStr.split(";")).forEach(v -> {
                        String[] array = v.split(":");
                        tableKey.put(array[0], array[1]);
                    });
                }
            }
        }
        return tableKey;
    }

    public Map<String, List<String>> getIgnoreColumns() {
        if (ignoreColumns == null) {
            synchronized(CanalMQConfig.class) {
                if (ignoreColumns == null) {
                    ignoreColumns = new HashMap<>();
                    Arrays.stream(tableIgnoreColumnsStr.split(";")).forEach(v -> {
                        String[] array = v.split(":");
                        ignoreColumns.put(array[0], Arrays.stream(array[1].split(",")).collect(Collectors.toList()));
                    });
                }
            }
        }
        return ignoreColumns;
    }
}
