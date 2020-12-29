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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.nopasserby.fastminimq.MQConsumer;
import org.nopasserby.fastminimq.MQQueue;
import org.nopasserby.fastminimq.MQResult;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.nopasserby.fastminimq.reliability.LocalDB.MQRecordCnt;

import io.netty.buffer.ByteBufUtil;

public class TransactionConsumerTest {
    
    String brokerHost = System.getProperty("brokerHost", "127.0.0.1:6001");
    
    int threadCount = Integer.parseInt(System.getProperty("threadCount", "8"));
    
    int topicCount = Integer.parseInt(System.getProperty("topicCount", "256"));
    
    String topicPrefix = System.getProperty("topicPrefix", "testTopic");
                                            
    String groups = System.getProperty("groups", "group1;group2");
    
    String targetGroups = System.getProperty("targetGroups", "group1;group2");
    
    LocalDB db = new LocalDB();
    
    int[] partition = partition(db); // partition is locked while updating rows
    
    public void run(String[] args) throws Exception {
        
        // Print start info
        StringBuilder startInfo = new StringBuilder(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        startInfo.append(" ready to start consumer throughput benchmark,server is ");
        startInfo.append(brokerHost);
        startInfo.append(",threadCount is:").append(threadCount);
        startInfo.append(",topicCount is:").append(topicCount);
        startInfo.append(",topicPrefix is:").append(topicPrefix);
        System.out.println(startInfo.toString());
        
        MQClusterMetaData clusterMetaData = buildClusterMetaData();
        MQConsumerCfg consumerCfg = buildConsumerCfg();
        MQConsumer consumer = new MQConsumer(consumerCfg, clusterMetaData);
        consumer.start();
        
        Queue<MQQueue> priorityQueue = new ConcurrentLinkedQueue<MQQueue>();
        for (MQQueue mqqueue: loadQueues()) {
            priorityQueue.add(mqqueue);
            System.out.printf("topic:%s, group:%s, subgroups:%d, subgroupNo:%d, step:%d, index:%d %n", 
                                  mqqueue.getTopic(), mqqueue.getGroup(), mqqueue.getSubgroups(), 
                                  mqqueue.getSubgroupNo(), mqqueue.getStep(), mqqueue.getIndex());
        }
        
        startupOperations(consumer, priorityQueue);
    }
    
    public List<MQQueue> loadQueues() {
        List<MQQueue> list = db.loadQueues();
        if (list != null && !list.isEmpty()) 
            return list;
        
        String[] groupArray = groups.split(";");
        String[] targetGroupArray = targetGroups.split(";");
        list = new ArrayList<MQQueue>();
        for (String topic: topicArray) {
            for (String targetGroup: targetGroupArray) {
                MQQueue mqqueue = createMQQueue(topic, groupArray, targetGroup);
                list.add(mqqueue);
                db.insert(mqqueue);
            }
        }
        return list;
    }
    
    AtomicInteger counter = new AtomicInteger();
    ThreadLocal<Integer> threadOrder = ThreadLocal.withInitial(() -> counter.incrementAndGet());
    
    public void startupOperations(MQConsumer consumer, Queue<MQQueue> priorityQueue) throws Exception {
        
        Runnable processor = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    MQQueue mqqueue = priorityQueue.poll();
                    if (mqqueue == null) {
                        LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                        continue;
                    }
                    long backup = mqqueue.getIndex();
                    try {
                        MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(mqqueue);
                        
                        MQResult<List<MQRecord>> mqResult = future.get(30, TimeUnit.SECONDS);
                        
                        mqqueue.ack();
                        
                        if (mqResult != null && mqResult.getResult().size() > 0) {
                            db.beginTransaction();
                            db.incrementRecordCnt(partition[threadOrder.get() % partition.length], mqResult.getResult().size());
                            db.update(mqqueue);
                            db.commit();
                            for (MQRecord record: mqResult.getResult())
                                System.out.printf("consume transaction id:%s, topic:%s, sign:%d status:%s %n", 
                                                   ByteBufUtil.hexDump(record.getId()), record.getTopic(), record.getSign(), record.getStatus());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        try {
                            db.rollback();
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                        mqqueue.setIndex(backup);
                        
                        LockSupport.parkNanos(SECONDS.toNanos(3));
                    }
                    priorityQueue.add(mqqueue);
                }
            }
        };
        
        for (int i = 0; i < threadCount; i++) {
            new Thread(processor, "Tx-Consumer-" + i).start();
        }
        
    }
    
    private MQConsumerCfg buildConsumerCfg() {
        return new MQConsumerCfg("testConsumer", "testCluster", "testBroker");
    }

    private MQClusterMetaData buildClusterMetaData() {
        MQClusterMetaData cluster = new MQClusterMetaData("testCluster");
        cluster.addBrokerMetaData(new MQBrokerMetaData("testBroker", brokerHost));
        return cluster;
    }
    
    public static MQQueue createMQQueue(String topic, String[] groupArray, String targetGroup) {
        MQQueue queue = new MQQueue();
        queue.setGroup(targetGroup);
        queue.setTopic(topic);
        queue.setSubgroups(groupArray.length);
        queue.setSubgroupNo(Arrays.asList(groupArray).indexOf(targetGroup));
        queue.setStep(200);
        return queue;
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
    
    String[] topicArray;
    {
        topicArray = new String[topicCount];
        for (int i = 0; i < topicCount; i++) {
            String topic = topicPrefix + String.format("%05d", i);
            topicArray[i] = topic;
        }
    }
    
    /**
     * vm args: -DbrokerHost=127.0.0.1:6001 -DdbLabel=testConsumer -Djdbc.configurationFile=/checker.properties
     * 
     * */
    public static void main(String[] args) throws Exception {
        TransactionConsumerTest tester = new TransactionConsumerTest();
        tester.run(args);
    }
    
}
