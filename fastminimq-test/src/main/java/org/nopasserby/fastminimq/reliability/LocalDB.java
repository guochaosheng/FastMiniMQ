/*
 * Copyright 2020 Guo Chaosheng
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nopasserby.fastminimq.reliability;

import static java.text.MessageFormat.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQQueue;
import org.nopasserby.fastminimq.MQResult.MQRecord;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.netty.buffer.ByteBufUtil;

public class LocalDB {
    
    final static String table_prefix = System.getProperty("tablePrefix", "");
    
    static String insert_record_sql = "insert into {0}transaction_record (`id`, `status`, `sign`, `broker`, `topic`, `body`, `timestamp`, `exception`) "
                                            + "values (''{1}'', {2}, {3}, ''{4}'', ''{5}'', ''{6}'', {7,number,#}, {8});";
    static String update_record_sql = "update {0}transaction_record "
                                            + "set `status`={2}, `sign`={3}, `broker`=''{4}'', `topic`=''{5}'', `body`=''{6}'', `timestamp`={7,number,#}, `exception`={8} "
                                            + "where `id` = ''{1}'';";
    static String delete_record_sql = "delete from {0}transaction_record where `id` = ''{1}'';";
    
    static String select_record_sql = "select * from {0}transaction_record;";
    
    static String select_timeout_record_sql = "select * from {0}transaction_record where `timestamp` < {1,number,#};";
    
    static String count_record_sql = "select count(*) as count from {0}transaction_record;";
    
    static String insert_queue_sql = "insert into {0}queue_record (`topic`, `group`, `subgroups`, `subgroup_no`, `step`, `index`) "
                                           + "values (''{1}'', ''{2}'', {3}, {4}, {5}, {6});";
    
    static String update_queue_sql = "update {0}queue_record set `index`={6,number,#} "
                                           + "where `topic`=''{1}'' and `group`=''{2}'' and `subgroups`={3} and `subgroup_no`={4} and `step`={5};";
    
    static String select_queue_sql = "select * from {0}queue_record;";
    
    static String update_record_cnt_sql = "update {0}transaction_record_cnt set `record_cnt` = `record_cnt` + {2} where `id` = {1};";
    
    static String select_record_cnt_sql = "select * from {0}transaction_record_cnt;";
    
    String configurationFile = System.getProperty("jdbcConfigurationFile", "/jdbc.properties");
    
    volatile DataSource dataSource;
    
    String label = System.getProperty("jdbcLabel", "");
    
    Properties props;
    
    public LocalDB() {
    }
    
    public LocalDB(String label, String configurationFile)  {
        this.label = label;
        this.configurationFile = configurationFile;
    }
    
    public String getLabel() {
        return this.label;
    }
    
    public String getHost() {
        String url = loadProps(label, configurationFile).getProperty("jdbcUrl");
        int beginIndex = url.indexOf("://") + "://".length();
        return url.substring(beginIndex , url.indexOf(":", beginIndex));
    }
    
    ThreadLocal<Connection> local = new ThreadLocal<Connection>();
    
    Connection newConnection(boolean usePoolConnection) throws SQLException {
        if (usePoolConnection) 
            return newPoolConnection();
        
        HikariConfig config = new HikariConfig(loadProps(label, configurationFile));
        return DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
    }
    
    Connection newPoolConnection() throws SQLException {
        Connection connection = local.get();
        if (connection != null) return connection;
        
        if (dataSource == null) {
            synchronized (LocalDB.class) {
                if (dataSource == null) {                    
                    dataSource = new HikariDataSource(new HikariConfig(loadProps(label, configurationFile)));
                }
            }
        }
        return dataSource.getConnection();
    }
    
    Properties loadProps(String label, String configurationFile)  {
        if (props != null) return props;
        
        Properties props = new Properties();
        File config = new File(configurationFile);
        try {
            props.load(config.isFile() ? new FileInputStream(config) : this.getClass().getResourceAsStream(configurationFile));
        } catch (IOException e) {
            throw new RuntimeException("label:" + label + " database properties file error", e);
        }
        if (label.isEmpty()) 
            return props;
        
        String prefix = label + ".";
        for (String key: props.stringPropertyNames()) {
            Object value = props.remove(key);
            if (key.startsWith(prefix)) {
                props.put(key.substring(prefix.length()), value);
            }
        }
        this.props = props;
        
        return props;
    }

    public void beginTransaction() throws SQLException {
        Connection conn = newPoolConnection();
        // need to store auto-commit status?
        conn.setAutoCommit(false);
        local.set(conn);
    }
    
    public void commit() throws SQLException {
        Connection conn = local.get();
        conn.commit();
        conn.setAutoCommit(true);
        conn.close();// moved to the pool
        local.remove();
    }
    
    public void rollback() throws SQLException {
        Connection conn = local.get();
        if (conn == null) return;
        conn.rollback();
        conn.setAutoCommit(true);
        conn.close();// moved to the pool
        local.remove();
    }
    
    public boolean insert(MQRecord record) {
        return execute(formatSQL(insert_record_sql, record));
    }
    
    public boolean insert(List<MQRecord> records) {
        StringBuilder batchSQL = new StringBuilder();
        for (MQRecord record: records) {
            batchSQL.append(formatSQL(insert_record_sql, record));
        }
        return execute(batchSQL.toString());
    }
    
    public boolean delete(MQRecord record) {
        return execute(formatSQL(delete_record_sql, record));
    }
    
    public boolean update(MQRecord record) {
        return execute(formatSQL(update_record_sql, record));
    }
    
    public boolean insert(MQQueue mqqueue) {
        return execute(formatSQL(insert_queue_sql, mqqueue));
    }
    
    public boolean incrementRecordCnt(int id, int cnt) {
        return execute(format(update_record_cnt_sql, table_prefix, id, cnt));
    }
    
    public boolean update(MQQueue mqqueue) {
        return execute(formatSQL(update_queue_sql, mqqueue));
    }
    
    static String formatSQL(String template, MQQueue queue) {
        return format(template, table_prefix,
                queue.getTopic(), queue.getGroup(), queue.getSubgroups(), queue.getSubgroupNo(), queue.getStep(), queue.getIndex());
    }
    
    static String formatSQL(String template, MQRecord record) {
        return format(template, table_prefix,
                ByteBufUtil.hexDump(record.getId()), record.getStatus().ordinal(), record.getSign(), 
                record.getBroker(), record.getTopic(), 
                new String(record.getBody()), record.getTimestamp(), null);
    }
    
    public long countRecords() {
        return executeQuery(format(count_record_sql, table_prefix), (rs) -> {
            return rs.getInt("count");
        }).remove(0);
    }
    
    public List<MQRecord> loadTimeOutRecords(long timestamp) {
        return executeQuery(format(select_timeout_record_sql, table_prefix, timestamp), (rs) -> {
            MQRecord record = new MQRecord();
            record.setId(ByteBufUtil.decodeHexDump(rs.getString("id")));
            record.setStatus(Status.valueOf(rs.getInt("status")));
            record.setSign(rs.getByte("sign"));
            record.setBroker(rs.getString("broker"));
            record.setTopic(rs.getString("topic"));
            record.setBody(rs.getString("body").getBytes());
            record.setTimestamp(rs.getLong("timestamp"));
            return record;
        });
    }
    
    public List<MQRecord> loadRecords() {
        return executeQuery(format(select_record_sql, table_prefix), (rs) -> {
            MQRecord record = new MQRecord();
            record.setId(ByteBufUtil.decodeHexDump(rs.getString("id")));
            record.setStatus(Status.valueOf(rs.getInt("status")));
            record.setSign(rs.getByte("sign"));
            record.setBroker(rs.getString("broker"));
            record.setTopic(rs.getString("topic"));
            record.setBody(rs.getString("body").getBytes());
            record.setTimestamp(rs.getLong("timestamp"));
            return record;
        });
    }
    
    public List<MQQueue> loadQueues() {
        return executeQuery(format(select_queue_sql, table_prefix), (rs) -> {
            MQQueue mqqueue = new MQQueue();
            mqqueue.setTopic(rs.getString("topic"));
            mqqueue.setGroup(rs.getString("group"));
            mqqueue.setSubgroups(rs.getInt("subgroups"));
            mqqueue.setSubgroupNo(rs.getInt("subgroup_no"));
            mqqueue.setStep(rs.getInt("step"));
            mqqueue.setIndex(rs.getInt("index"));
            return mqqueue;
        });
    }
    
    public List<MQRecordCnt> loadRecordCnt() {
        return executeQuery(format(select_record_cnt_sql, table_prefix), (rs) -> {
            MQRecordCnt recordCnt = new MQRecordCnt();
            recordCnt.setId(rs.getInt("id"));
            recordCnt.setCnt(rs.getLong("record_cnt"));
            return recordCnt;
        });
    }
    
    boolean execute(String sql) {
        return execute(sql, true);
    }
    
    boolean execute(String sql, boolean usePoolConnection) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = newConnection(usePoolConnection);
            stmt = conn.createStatement();
            for (String s: sql.split(";")) {
                if (s.trim().isEmpty()) continue;
                stmt.addBatch(s);
            }
            stmt.executeBatch();
            return true;
        } catch (Exception e) {
            throw new AccessException("SQLException:" + sql, e);
        } finally {
            close(stmt);
            if (conn != local.get()) close(conn);
        }
    }

    <T> List<T> executeQuery(String sql, RsFunction<ResultSet, T> function) {
        List<T> list = new ArrayList<T>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = newPoolConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(function.apply(rs));
            }
        } catch (Exception e) {
            throw new AccessException("SQLException:" + sql, e);
        } finally {
            close(rs, stmt);
            if (conn != local.get()) close(conn);
        };
        return list;
    }
    
    void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable: closeables) {
            try {
                if (closeable != null) closeable.close();
            } catch (Exception e) {
                throw new AccessException("IOException:" + closeable.getClass().getSimpleName() + " cannot be closed", e);
            }
        }
    }
    
    @FunctionalInterface
    public static interface RsFunction<T, R> {
        public R apply(T t) throws Exception;
    }
    
    public static class AccessException extends RuntimeException {
        static final long serialVersionUID = -8404750257613867560L;
        public AccessException(String msg, Exception e) {
            super(msg, e); 
        }
    }
    
    public static class MQRecordCnt {
        int id;
        long cnt;
        
        public int getId() {
            return id;
        }
        public void setId(int id) {
            this.id = id;
        }
        public long getCnt() {
            return cnt;
        }
        public void setCnt(long cnt) {
            this.cnt = cnt;
        }
    }
    
}
