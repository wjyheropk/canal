package com.alibaba.otter.canal.server;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.instance.core.CanalConfig;
import com.alibaba.otter.canal.instance.core.CanalMQProducer;
import com.alibaba.otter.canal.instance.core.FullTableFetcher;
import com.alibaba.otter.canal.kafka.CanalKafkaProducer;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 1、对每个表进行锁表
 * 2、对每个表进行select * from (mysqldump 在有些权限下不能用)
 * 3、show master status
 * 4、释放所有表
 * 5、更新增量同步的start position
 *
 * @author wangjiayin
 * @since 2018/11/14
 */
@Slf4j
@Data
public class JdbcFullTableFetcher implements FullTableFetcher {

    private ExecutorService executorService;

    private String dbAddress;

    private String dbUsername;

    private String dbPassword;

    private String dbName;

    private boolean syncOnStart;

    private String tables;

    private List<TableInfo> tableInfos;

    public void init() {
        // 同时有多个table执行同步，设置太大怕内存爆掉
        executorService = Executors.newFixedThreadPool(3);
        tableInfos = new ArrayList<>();
        Arrays.stream(tables.split(";")).forEach(conf -> {
            String[] array = conf.split(",");
            tableInfos.add(new TableInfo(array[0], array[1], null, array[2]));
        });
    }

    @Override
    public boolean syncOnStart() {
        return syncOnStart;
    }

    @Override
    public EntryPosition start() {

        CanalMQProducer producer = new CanalKafkaProducer();
        producer.init(CanalConfig.getInstance().getMqProperties());

        String jdbcUrl = "jdbc:mysql://" + dbAddress + "/" + dbName;
        DataSource dataSource = this.newDataSource(jdbcUrl, dbUsername, dbPassword);

        try {

            Map<String, Future<Boolean>> futures = new HashMap<>();
            for (TableInfo t : tableInfos) {
                Task task = new Task(dataSource, t, producer);
                Future<Boolean> future = executorService.submit(task);
                futures.put(t.name, future);
            }

            boolean hasError = false;
            for (String table : futures.keySet()) {
                try {
                    if (futures.get(table).get()) {
                        continue;
                    }
                    log.warn("sync task failed, table={}", table);
                } catch (Exception e) {
                    log.warn("sync task failed, table=" + table, e);
                }
                hasError = true;
                break;
            }

            if (hasError) {
                futures.values().forEach(f -> f.cancel(true));
            }

            return findNewPosition(dataSource);

        } catch (Exception e) {
            log.warn("fail to make full table sync.", e);
            throw new RuntimeException("fail to make full table sync.");
        } finally {
            try {
                // 关闭线程池
                executorService.shutdown();
                // 解锁表
                Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("unlock tables");
                ps.execute();
                // 释放资源
                conn.close();
                if (dataSource instanceof Closeable) {
                    ((Closeable) dataSource).close();
                }
                // producer先不关
            } catch (Exception e) {
                log.warn("fail to make full table sync.", e);
            }
        }

    }

    private DataSource newDataSource(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(20);
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("autoReconnect", "true");
        config.addDataSourceProperty("failOverReadOnly", "false");
        config.setConnectionTimeout(600000);
        return new HikariDataSource(config);
    }

    private EntryPosition findNewPosition(DataSource dataSource) throws Exception {
        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("show master status");
        ResultSet rs = ps.executeQuery();
        rs.next();
        EntryPosition position = new EntryPosition();
        position.setJournalName(rs.getString("File"));
        position.setPosition(Long.valueOf(rs.getString("Position")));
        position.setTimestamp(System.currentTimeMillis());
        conn.close();
        return position;
    }

    public static class Task implements Callable<Boolean> {

        private DataSource dataSource;

        private TableInfo t;

        private int batchSize = 100000;

        private CanalMQProducer producer;

        public Task(DataSource dataSource, TableInfo t, CanalMQProducer producer) {
            this.dataSource = dataSource;
            this.t = t;
            this.producer = producer;
        }

        @Override
        public Boolean call() throws Exception {
            Connection conn = dataSource.getConnection();
            String name = t.name;
            String pk = t.pk;
            Long timestamp = System.currentTimeMillis();
            log.info("begin to full sync table={}", name);
            // 锁表
            String sql = String.format("lock tables %s read", name);
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
            // 开始同步
            sql = String.format("select max(%s) as id from %s", pk, name);
            ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            rs.next();
            Long maxPK = rs.getLong("id");
            log.info("table {} maxPk={}", name, maxPK);
            for (long i = 1; i < maxPK; i += batchSize) {
                sql = t.sql == null ? String.format("select * from %s where %s between %d and %d",
                        name, pk, i, i + batchSize) : t.sql;
                ps = conn.prepareStatement(sql);
                List<Map<String, String>> data = getData(ps.executeQuery());
                // 转换未key-value形式
                List<CanalMQProducer.KeyValue> keyVales = data.stream().map(map -> {
                    String key = map.get(pk);
                    String value = JsonUtils.marshalToString(map);
                    return new CanalMQProducer.KeyValue(key, value);
                }).collect(Collectors.toList());
                producer.send(t.kafkaTopic, timestamp, keyVales);
                log.info("begin [{}, {}], total={}", i, i + batchSize, maxPK);
            }
            log.info("full sync table={} success", name);
            return true;
        }

        private List<Map<String, String>> getData(ResultSet rs) throws SQLException {
            List<Map<String, String>> results = Lists.newArrayList();
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            List<String> colNameList = Lists.newArrayList();
            for (int i = 0; i < colCount; i++) {
                colNameList.add(rsmd.getColumnName(i + 1));
            }
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                for (int i = 0; i < colCount; i++) {
                    String key = colNameList.get(i);
                    String value = rs.getString(colNameList.get(i));
                    map.put(key, value);
                }
                results.add(map);
            }
            return results;
        }
    }

    @AllArgsConstructor
    public static class TableInfo {
        private String name;
        private String pk;
        private String sql;
        private String kafkaTopic;
    }

}