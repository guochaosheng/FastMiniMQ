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

import static java.lang.Thread.sleep;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Checker {
    
    static final String configurationFile = System.getProperty("checker.configurationFile", "/checker.properties");
    
    MultiDB multiDB = new MultiDB();
    
    public void load(String configurationFile) throws IOException {
        multiDB.load(configurationFile);
    }
    
    public void start() throws Exception {
        Stats stats = new Stats(multiDB);
        stats.waitFor();
        stats.print();
    }
    
    static class Stats {
        MultiDB multiDB;
        public Stats(MultiDB multiDB) {
            this.multiDB = multiDB;
        }
        
        public void waitFor() throws InterruptedException {
            long lastRCnt = -1;
            while (true) {
                long rCnt = 0;
                for (LocalDB localDB: multiDB.consumerGroup) {
                    rCnt += localDB.loadRecordCnt().stream().mapToLong((recordCnt) -> recordCnt.getCnt()).sum();
                }
                if (lastRCnt > 0 && lastRCnt == rCnt) break;
                if (lastRCnt > 0 && lastRCnt != rCnt) sleep(120 * 1000);
                lastRCnt = rCnt;
                sleep(100);
            }
        }
        
        /**
        * Print Example:
        *
        *  Report (category:producer, date:2020-12-28) 
        * +-----------+-----------+-------------+-----------+
        * |node name  |node host  |unconfirmed  |confirmed  |
        * +-----------+-----------+-------------+-----------+
        * |producer1  |localhost  |4            |171403     |
        * +-----------+-----------+-------------+-----------+
        * 
        *  Report (category:consumer, date:2020-12-28) 
        * +-----------+-----------+-----------+
        * |node name  |node host  |confirmed  |
        * +-----------+-----------+-----------+
        * |consumer1  |localhost  |171403     |
        * +-----------+-----------+-----------+
        * 
        *  Report (category:aggregation, date:2020-12-28) 
        * +----------------------------+--------+
        * |label                       |count   |
        * +----------------------------+--------+
        * |Producer Group Unconfirmed  |4       |
        * |Producer Group confirmed    |171403  |
        * +----------------------------+--------+
        * |Consumer Group confirmed    |171403  |
        * +----------------------------+--------+
        * |Check Result                |true    |
        * +----------------------------+--------+
        * 
        */
        public void print() throws Exception {
            long rCnt = 0;
            int rn = multiDB.consumerGroup.size();
            String[][] consumerGroupTable = new String[rn + 1][3];
            consumerGroupTable[0] = new String[] {"node name", "node host", "confirmed"};
            for (int i = 0; i < rn; i++) {
                LocalDB localDB = multiDB.consumerGroup.get(i);
                long cnt = localDB.loadRecordCnt().stream().mapToLong((recordCnt) -> recordCnt.getCnt()).sum();
                rCnt += cnt;
                consumerGroupTable[i + 1][0] = localDB.getLabel(); 
                consumerGroupTable[i + 1][1] = localDB.getHost();
                consumerGroupTable[i + 1][2] = cnt + "";
            }
            
            
            long pUnCnt = 0;
            long pCnt = 0;
            int pn = multiDB.producerGroup.size();
            String[][] producerGroupTable = new String[pn + 1][4];
            producerGroupTable[0] = new String[] {"node name", "node host", "unconfirmed", "confirmed"};
            for (int i = 0; i < pn; i++) {
                LocalDB localDB = multiDB.producerGroup.get(i);
                long unCnt = localDB.countRecords();
                long cnt = localDB.loadRecordCnt().stream().mapToLong((recordCnt) -> recordCnt.getCnt()).sum();                
                pUnCnt += unCnt;
                pCnt += cnt;
                producerGroupTable[i + 1][0] = localDB.getLabel(); 
                producerGroupTable[i + 1][1] = localDB.getHost();
                producerGroupTable[i + 1][2] = unCnt + "";
                producerGroupTable[i + 1][3] = cnt + "";
            }
            
            String[][] summaryTable = new String[7][2];
            summaryTable[0] = new String[] { "label", "count" };
            summaryTable[1] = new String[] { "Producer Group Unconfirmed", pUnCnt + ""};
            summaryTable[2] = new String[] { "Producer Group confirmed", pCnt + ""};
            summaryTable[3] = PrintUtil.LINE_SEPARATOR_REFERENCE;
            summaryTable[4] = new String[] { "Consumer Group confirmed", rCnt + ""};
            summaryTable[5] = PrintUtil.LINE_SEPARATOR_REFERENCE;
            summaryTable[6] = new String[] { "Check Result", startupCheck(pUnCnt, pCnt, rCnt) + ""};
            
            LocalDate now = LocalDate.now();
            System.out.printf("%n Report (category:producer, date:%s) %n", now);
            PrintUtil.print(producerGroupTable);
            System.out.printf("%n Report (category:consumer, date:%s) %n", now);
            PrintUtil.print(consumerGroupTable);
            System.out.printf("%n Report (category:aggregation, date:%s) %n", now);
            PrintUtil.print(summaryTable);
        }
        
        /**
         * 核对数量时分成如下 2 种情况：
         * 
         * 1. 当 Producer 侧存在已开始第二阶段投递但未确认投递成功消息，此时不同状态消息数量满足如下关系
         * * Producer 侧已完成投递事务消息总数量 <= Consumer 侧已消费处理消息总数量
         * * Producer 侧已完成投递事务消息总数量 + 已开始第二阶段投递但未确认投递成功消息总数量 >= Consumer 侧已消费处理消息总数量
         * 
         * 2. 当 Producer 侧全部事务消息投递成功（或者不存在已开始第二阶段投递但未确认投递成功消息），此时不同状态消息数量满足如下关系
         * * Producer 侧已完成投递事务消息总数量 = Consumer 侧已消费处理消息总数量
         */
        boolean startupCheck(long producerUnConfirmedRecordCnt, long producerConfirmedRecordCnt, long consumerConfirmedRecordCnt) {
            if (producerUnConfirmedRecordCnt > 0) {
                if (producerConfirmedRecordCnt <= consumerConfirmedRecordCnt 
                        && (producerConfirmedRecordCnt + producerUnConfirmedRecordCnt) >= consumerConfirmedRecordCnt) {
                    return true;
                }
            } else {
                if (producerConfirmedRecordCnt == consumerConfirmedRecordCnt) {
                    return true;
                }
            }
            return false;
        }
    }

    static class MultiDB {
        List<LocalDB> producerGroup;
        List<LocalDB> consumerGroup;
        
        public void load(String file) throws IOException {
            Properties props = new Properties();
            final File propsfile = new File(file);
            props.load(propsfile.isFile() ? new FileInputStream(propsfile) : this.getClass().getResourceAsStream(file));
            
            String[] producers = props.getProperty("producer.label").split(",");
            String[] consumers = props.getProperty("consumer.label").split(",");
            producerGroup = createLocalDBList(producers, file);
            consumerGroup = createLocalDBList(consumers, file);
        }
        
        public List<LocalDB> createLocalDBList(String[] groupids, String jdbcConfigurationFile) {
            List<LocalDB> producerGroup = new ArrayList<LocalDB>();
            for (String groupid: groupids) {
                producerGroup.add(new LocalDB(groupid, jdbcConfigurationFile));
            }
            return producerGroup;
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Checker checker = new Checker();
        checker.load(configurationFile);
        checker.start();
    }

}
