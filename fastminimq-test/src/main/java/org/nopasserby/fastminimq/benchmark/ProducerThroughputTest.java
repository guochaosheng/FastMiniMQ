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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class ProducerThroughputTest {
    
    String brokerHost = System.getProperty("ip", "127.0.0.1") + ":" + System.getProperty("port", "6001");
    
    int threadCount = Integer.parseInt(System.getProperty("threadCount", "4"));
    
    int messageSize = Integer.parseInt(System.getProperty("messageSize", "128"));
    
    int topicCount = Integer.parseInt(System.getProperty("topicCount", "256"));
    
    String topicPrefix = System.getProperty("topicPrefix", "testTopic");
    
    long messageCount = Long.parseLong(System.getProperty("messageCount", Long.MAX_VALUE + ""));
    
    StatsBenchmark statsBenchmark = new StatsBenchmark();
    
    MessageGenerator messageGenerator = new MessageGenerator();
    
    public void run(String[] args) throws Exception {
        
        // Print start info
        StringBuilder startInfo = new StringBuilder(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        startInfo.append(" ready to start producer throughput benchmark,server is ");
        startInfo.append(brokerHost);
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
        statsBenchmark.start();
        
        startupOperations(producer);
        
    }
    
    
    public void startupOperations(MQProducer producer) throws Exception {
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        AtomicLong remainCounter = new AtomicLong(messageCount);
        
        Runnable processor = new Runnable() {
            
            final Consumer<MQRecord> callback = new Consumer<MQRecord>() {
                @Override
                public void accept(MQRecord record) {
                    if (record.getStatus() == Status.OK) {                      
                        statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
                    } else {
                        statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                        delay = true;
                    }
                    
                    statsBenchmark.offerCurrentRT(calculateRT(record));
                    
                    if (record.getException() instanceof IllegalStateException) {
                        shutdown = true;
                    }
                }
            };
            
            volatile boolean delay;
            
            volatile boolean shutdown;
            
            @Override
            public void run() {
                while (!shutdown && remainCounter.decrementAndGet() >= 0) {
                    try {
                        
                        if (delay) {
                            Thread.sleep(10);
                            delay = false;
                        }
                        
                        String topic = messageGenerator.currentTopic();
                        byte[] body = messageGenerator.currentBody();
                        MQFuture<MQRecord> future = producer.sendMsg(topic, body);
                        future.thenAccept(callback);
                        
                        statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                        
                        messageGenerator.next();
                    } catch (Exception e) {
                        statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                    }
                }
                latch.countDown();
            }
        };
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.execute(processor);
        }
        latch.await();
        
        executor.shutdown();
    }
    
    private MQProducerCfg buildProducerCfg(MQClusterMetaData clusterMetaData) {
        return new MQProducerCfg("testProducer", clusterMetaData.name());
    }

    private MQClusterMetaData buildClusterMetaData() {
        MQClusterMetaData clusterMetaData = new MQClusterMetaData("testCluster");
        clusterMetaData.addBrokerMetaData(new MQBrokerMetaData("testBroker", brokerHost));
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
    
    long calculateRT(MQRecord record) {
        long currentTimeMillis = System.currentTimeMillis();
        long sendTimeMillis = (ByteBuffer.wrap(record.getId()).getLong(8) >> 22) + 1288834974657L;// valid only if mqutil.nextuuid is used
        return currentTimeMillis - sendTimeMillis;
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
    
    public static void main(String[] args) throws Exception {
        ProducerThroughputTest tester = new ProducerThroughputTest();
        tester.run(args);
    }
    
}
