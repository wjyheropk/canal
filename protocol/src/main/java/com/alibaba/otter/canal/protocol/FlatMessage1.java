package com.alibaba.otter.canal.protocol;

import lombok.Data;

/**
 * @author wangjiayin
 * @since 2018/11/13
 */
@Data
public class FlatMessage1 {

    // 格式：database.tableName
    private String table;
    // binlog executeTime
    private Long es;

    private String key;

    private String value;

    public FlatMessage1(String table, Long es, String key, String value) {
        this.table = table;
        this.es = es;
        this.key = key;
        this.value = value;
    }
}
