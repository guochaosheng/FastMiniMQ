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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import java.net.NoRouteToHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.nopasserby.fastminimq.MQConstants.Transaction;
import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.nopasserby.fastminimq.reliability.LocalDB.MQRecordCnt;

import io.netty.buffer.ByteBufUtil;

public class TransactionProducerTest {
    
    String brokerHosts = System.getProperty("brokerHosts", "127.0.0.1:6001;");
    
    int threadCount = Integer.parseInt(System.getProperty("threadCount", "4"));
    
    int messageSize = Integer.parseInt(System.getProperty("messageSize", "128"));
    
    int topicCount = Integer.parseInt(System.getProperty("topicCount", "256"));
    
    String topicPrefix = System.getProperty("topicPrefix", "testTopic");
    
    long messageCount = Long.parseLong(System.getProperty("messageCount", Long.MAX_VALUE + ""));
    
    MessageGenerator messageGenerator = new MessageGenerator();
    
    LocalDB db = new LocalDB();
    
    int[] partition = partition(db); // partition lock when updating row records
    
    public void run(String[] args) throws Exception {
        
        // Print start info
        StringBuilder startInfo = new StringBuilder(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        startInfo.append(" ready to start transaction producer,server is ");
        startInfo.append(brokerHosts);
        startInfo.append(",threadCount is:").append(threadCount);
        startInfo.append(",messageSize is:").append(messageSize);
        startInfo.append(",messageCount is:").append(messageCount);
        startInfo.append(",topicCount is:").append(topicCount);
        startInfo.append(",topicPrefix is:").append(topicPrefix);
        System.out.println(startInfo.toString());
        
        MQClusterMetaData cluster = buildClusterMetaData();
        MQProducerCfg producerCfg = buildProducerCfg(cluster);
        MQProducer producer = new MQProducer(producerCfg, cluster);
        producer.start();
        
        startupOperations(producer);
        
        makeupOperations(producer);
        
    }
    
    public void makeupOperations(MQProducer producer) {
        Timer timer = new Timer("Tx-Producer-MakeUp-Thread");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Set<String> failureBroker = new HashSet<String>();
                List<MQRecord> records = db.loadTimeOutRecords(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));
                for (MQRecord record: records) {
                    if (failureBroker.contains(record.getBroker())) continue; // ignore fault node
                    
                    boolean confirmed = confirmTransaction(producer, record);
                    if (!confirmed) failureBroker.add(record.getBroker());
                }
            }
        }, 0L, 1000L);
    }

    public void startupOperations(MQProducer producer) throws Exception {
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        AtomicLong remainCounter = new AtomicLong(messageCount);
        
        Runnable processor = new Runnable() {
            @Override
            public void run() {
                while (remainCounter.decrementAndGet() >= 0) {
                    String topic = messageGenerator.currentTopic();
                    byte[] body = messageGenerator.currentBody();
                    
                    MQRecord record = prepareTransaction(producer, topic, body);
                    if (record.getStatus() == Status.OK)
                        confirmTransaction(producer, record);
                    if (record.getStatus() == Status.FAIL) 
                        parkNanos(SECONDS.toNanos(3));
                    
                    messageGenerator.next();
                }
                latch.countDown();
            }
        };
        
        for (int i = 0; i < threadCount; i++) {
            new Thread(processor, "Tx-Producer-" + i).start();
        }
        latch.await();
    }
    
    private MQRecord prepareTransaction(MQProducer producer, String topic, byte[] body) {
        try {
            MQRecord record = producer.sendTxMsg(topic, body).get(30, TimeUnit.SECONDS);
            record.setTimestamp(System.currentTimeMillis());
            // record unnecessary rollback when it has no routing address
            if (!(record.getException() instanceof NoRouteToHostException)) 
                db.insert(record);
            
            System.out.printf("prepare transaction id:%s, topic:%s, broker:%s, sign:%d, status:%s %n", 
                    ByteBufUtil.hexDump(record.getId()), record.getTopic(), record.getBroker(), record.getSign(), record.getStatus());
            
            return record;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    AtomicInteger counter = new AtomicInteger();
    ThreadLocal<Integer> threadOrder = ThreadLocal.withInitial(() -> counter.incrementAndGet());
    
    private boolean confirmTransaction(MQProducer producer, MQRecord record) {
        try {
            Status status = record.getStatus();
            if (status == Status.OK) {
                record = producer.commit(record).get(30, TimeUnit.SECONDS);
            } else {
                record = producer.rollback(record).get(30, TimeUnit.SECONDS);
            }
            
            if (record.getStatus() == Status.OK) {
                db.beginTransaction();
                boolean committed = record.getSign() == Transaction.COMMIT.ordinal() && record.getStatus() == Status.OK;
                if (committed) db.incrementRecordCnt(partition[threadOrder.get() % partition.length], 1);
                db.delete(record);
                db.commit();
            }
            
            System.out.printf("confirm transaction id:%s, topic:%s, broker:%s, sign:%d, status:%s %n", 
                    ByteBufUtil.hexDump(record.getId()), record.getTopic(), record.getBroker(), record.getSign(), record.getStatus());
            
            return record.getStatus() == Status.OK;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    private MQProducerCfg buildProducerCfg(MQClusterMetaData clusterMetaData) {
        return new MQProducerCfg("testProducer", clusterMetaData.name());
    }

    private MQClusterMetaData buildClusterMetaData() {
        MQClusterMetaData clusterMetaData = new MQClusterMetaData("testCluster");
        int sequence = 0;
        for (String brokerHost: brokerHosts.split(";")) {
            clusterMetaData.addBrokerMetaData(new MQBrokerMetaData("testBroker-" + (sequence++), brokerHost));
        }
        return clusterMetaData;
    }
    
    private static byte[] buildBody(String topic, int messageSize) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(topic);
        byte[] topicBytes = topic.getBytes();
        int remaining = messageSize - topicBytes.length;
        
        byte[] salt = " Hello world ".getBytes();
        for (int i = 0; i < remaining; i++) {
            stringBuilder.append((char) salt[i % salt.length]);
        }
        return stringBuilder.toString().getBytes();
    }
    
    public int[] partition(LocalDB db) {
        List<MQRecordCnt> recordCntList = db.loadRecordCnt();
        int n = recordCntList.size();
        int[] partition = new int[n];
        for (int i = 0; i < n; i++) {
            partition[i] = recordCntList.get(i).getId();
        }
        return partition;
    }
    
    class MessageGenerator {
        String[] topicArray;
        byte[][] bodyArray;
        int sequence;
        void next() {
            sequence = (++sequence) < 0 ? 0 : sequence;
        }
        String currentTopic() {
            return topicArray[sequence % topicArray.length];
        }
        byte[] currentBody() {
            return bodyArray[sequence % bodyArray.length];
        }
        String[] topicArray() {
            return topicArray;
        }
        {
            topicArray = new String[topicCount];
            bodyArray = new byte[topicCount][];
            for (int i = 0; i < topicCount; i++) {
                String topic = topicPrefix + String.format("%05d", i);
                topicArray[i] = topic;
                bodyArray[i] = buildBody(topic, messageSize);
            }
        }
    }
    
    /**
     * vm args: -DbrokerHosts=127.0.0.1:6001;127.0.0.1:6002; -Djdbc.Label=testProducer -DmessageCount=10000 -Djdbc.configurationFile=/checker.properties
     * 
     * */
    public static void main(String[] args) throws Exception {
        TransactionProducerTest tester = new TransactionProducerTest();
        tester.run(args);
    }
    
}
