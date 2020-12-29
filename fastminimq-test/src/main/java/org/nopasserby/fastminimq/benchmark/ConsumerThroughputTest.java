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

package org.nopasserby.fastminimq.benchmark;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.nopasserby.fastminimq.MQConsumer;
import org.nopasserby.fastminimq.MQQueue;
import org.nopasserby.fastminimq.MQResult;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class ConsumerThroughputTest {
    
    String brokerHost = System.getProperty("ip", "127.0.0.1") + ":" + System.getProperty("port", "6001");
    
    int threadCount = Integer.parseInt(System.getProperty("threadCount", "8"));
    
    int topicCount = Integer.parseInt(System.getProperty("topicCount", "256"));
    
    String topicPrefix = System.getProperty("topicPrefix", "testTopic");
    
    StatsBenchmark statsBenchmark = new StatsBenchmark();
    
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
        statsBenchmark.start();
        
        startupOperations(consumer);
        
    }
    
    public void startupOperations(MQConsumer consumer) throws Exception {
        ConcurrentLinkedQueue<MQQueue> priorityQueue = new ConcurrentLinkedQueue<MQQueue>();
        for (String topic: topicArray) {
            priorityQueue.add(createMQQueue(topic, MQGroup.GROUP_1));
        }
        
        Runnable processor = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        MQQueue mqqueue = priorityQueue.poll();
                        if (mqqueue == null) {
                            continue;
                        }
                        MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(mqqueue);
                        statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                        
                        MQResult<List<MQRecord>> result = future.get(30, TimeUnit.SECONDS);
                        if (result == null) {
                            statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                        } else {
                            List<MQRecord> recordList = result.getResult();
                            statsBenchmark.getReceiveResponseSuccessCount().addAndGet(recordList.size());
                            for (MQRecord record: recordList) {
                                statsBenchmark.offerCurrentRT(calculateRT(record));
                            }
                        }
                        
                        mqqueue.ack();
                         
                        priorityQueue.add(mqqueue);
                    } catch (Exception e) {
                        statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                    }
                }
            }
        };
        
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(processor);
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
    
    public static MQQueue createMQQueue(String topic, MQGroup group) {
        MQQueue queue = new MQQueue();
        MQGroup[] groups = MQGroup.values();
        queue.setGroup(MQGroup.class.getSimpleName());
        queue.setTopic(topic);
        queue.setSubgroups(groups.length);
        queue.setSubgroupNo(group.ordinal());
        queue.setStep(200);
        return queue;
    }
    
    enum MQGroup { GROUP_1 }
    
    String[] topicArray;
    {
        topicArray = new String[topicCount];
        for (int i = 0; i < topicCount; i++) {
            String topic = topicPrefix + String.format("%05d", i);
            topicArray[i] = topic;
        }
    }
    
    long calculateRT(MQRecord record) {
        long currentTimeMillis = System.currentTimeMillis();
        long sendTimeMillis = (ByteBuffer.wrap(record.getId()).getLong(8) >> 22) + 1288834974657L;// valid only if mqutil.nextuuid is used
        return currentTimeMillis - sendTimeMillis;
    }
    
    public static void main(String[] args) throws Exception {
        ConsumerThroughputTest tester = new ConsumerThroughputTest();
        tester.run(args);
    }
    
}
