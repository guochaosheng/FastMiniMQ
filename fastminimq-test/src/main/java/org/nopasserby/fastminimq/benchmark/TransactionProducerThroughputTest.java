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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class TransactionProducerThroughputTest {
    
    String brokerHost = System.getProperty("ip", "127.0.0.1") + ":" + System.getProperty("port", "6001");
    
    int messageSize = Integer.parseInt(System.getProperty("messageSize", "128"));
    
    int topicCount = Integer.parseInt(System.getProperty("topicCount", "256"));
    
    String topicPrefix = System.getProperty("topicPrefix", "testTopic");
    
    StatsBenchmark statsBenchmark = new StatsBenchmark();
    
    MessageGenerator messageGenerator = new MessageGenerator();
    
    public void run(String[] args) throws Exception {
        
        // Print start info
        StringBuilder startInfo = new StringBuilder(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        startInfo.append(" ready to start transaction producer throughput benchmark,server is ");
        startInfo.append(brokerHost);
        startInfo.append(",messageSize is:").append(messageSize);
        startInfo.append(",topicCount is:").append(topicCount);
        startInfo.append(",topicPrefix is:").append(topicPrefix);
        System.out.println(startInfo.toString());
        
        MQClusterMetaData clusterMetaData = buildClusterMetaData();
        MQProducerCfg producerCfg = buildProducerCfg(clusterMetaData);
        MQProducer producer = new MQProducer(producerCfg, clusterMetaData);
        producer.start();
        statsBenchmark.start();
        
        BlockingQueue<MQRecord> preparedQueue = new ArrayBlockingQueue<MQRecord>(1024 * 64);
        
        AtomicInteger blance = new AtomicInteger();
        
        CyclicBarrier startBarrier = new CyclicBarrier(2);
        
        startupPreparedOperations(producer, blance, preparedQueue, startBarrier);
        
        startupCommitOperations(producer, blance, preparedQueue, startBarrier);
        
    }

    public void startupPreparedOperations(MQProducer producer, AtomicInteger blance, BlockingQueue<MQRecord> preparedQueue, CyclicBarrier startBarrier) throws Exception {
        
        Runnable processor = new Runnable() {
            final Consumer<MQRecord> callback = new Consumer<MQRecord>() {
                @Override
                public void accept(MQRecord record) {
                    if (record.getStatus() == Status.OK) {                        
                        preparedNextStep(record);
                        
                        statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                    } else {
                        statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                    }
                }
                
                void preparedNextStep(MQRecord record) {
                    try {
                        preparedQueue.put(record);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("queue put operation interrupted", e);
                    }
                }
            };
            
            @Override
            public void run() {
                
                waitFor(startBarrier);
                
                while (true) {
                    if (blance.get() > 1024 * 64 * 2) {
                        waitFor();
                        continue;
                    }
                    try {
                        String topic = messageGenerator.currentTopic();
                        byte[] body = messageGenerator.currentBody();
                        
                        MQFuture<MQRecord> future = producer.sendTxMsg(topic, body);
                        future.thenAccept(callback);
                        blance.incrementAndGet();
                        
                        messageGenerator.next();
                    } catch (Exception e) {
                        e.printStackTrace();
                        statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                    }
                }
            }
        };
        
        new Thread(processor).start();
    }

    public void startupCommitOperations(MQProducer producer, AtomicInteger blance, BlockingQueue<MQRecord> preparedQueue, CyclicBarrier startBarrier) throws Exception {
        
        Runnable processor = new Runnable() {

            final Consumer<MQRecord> callback = new Consumer<MQRecord>() {
                @Override
                public void accept(MQRecord record) {
                    if (record.getStatus() == Status.OK) {
                        statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
                    } else {
                        statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                    }
                    
                    statsBenchmark.offerCurrentRT(calculateRT(record));
                }

            };
            
            @Override
            public void run() {
                try {
                    while (true) {
                        MQRecord record = preparedQueue.take();
                        MQFuture<MQRecord> future = producer.commit(record);
                        blance.decrementAndGet();
                        future.thenAccept(callback);
                    }
                } catch (Exception e) {
                    statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                }
            }
        };
        
        waitFor(startBarrier);
        
        new Thread(processor).start();
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
    
    void waitFor() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    void waitFor(CyclicBarrier startBarrier) {
        try {
            startBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }
    
    
    class MessageGenerator {
        String[] topicArray;
        byte[][] bodyArray;
        int sequence;
        void next() {
            sequence = (++sequence) % topicCount;
        }
        String currentTopic() {
            return topicArray[sequence];
        }
        byte[] currentBody() {
            return bodyArray[sequence];
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
        TransactionProducerThroughputTest tester = new TransactionProducerThroughputTest();
        tester.run(args);
    }
    
}
