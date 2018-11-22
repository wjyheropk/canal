package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * 全量表同步器
 *
 * @author wangjiayin
 * @since 2018/11/15
 */
public interface FullTableFetcher {

    /**
     * 启动时，是否执行全量同步
     */
    boolean syncOnStart();

    /**
     * 开始执行同步
     *
     * @return 全量同步后的binlog解析位置
     */
    EntryPosition start();

}
