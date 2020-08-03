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

package org.nopasserby.fastminimq.example;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQRegistry;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class MQProducerTest {
    
    public static void main(String[] args) throws Exception {
        MQClusterMetaData clusterMetaData = MQRegistry.loadClusterMetaData("cluster-test::broker-test@127.0.0.1:6001;");
        
        MQProducerCfg producerCfg = new MQProducerCfg("producer-test", "cluster-test");
        MQProducer producer = new MQProducer(producerCfg, clusterMetaData);
        producer.start();
        
        String topic = "testTopic";
        int batchCount = 500;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        
        // first batch
        for (int i = 0; i < batchCount; i++) {
            String body = "message no:" + i + ", datetime:" + sdf.format(new Date());
            MQFuture<MQRecord> future = producer.sendMsg(topic, body.getBytes());
            MQRecord record = future.get(30, TimeUnit.SECONDS);
            if (record.getStatus() == Status.OK) {
                System.out.printf("[%s] send message success.%n", body);
            } else {
                System.out.printf("[%s] send message failed.%n", body);
            }
        }
        
        Thread.sleep(10 * 60 * 1000);
        
        // second batch
        for (int i = 0, j = batchCount; i < batchCount; i++, j++) {
            String body = "message no:" + j + ", datetime:" + sdf.format(new Date());
            MQFuture<MQRecord> future = producer.sendMsg(topic, body.getBytes());
            MQRecord record = future.get(30, TimeUnit.SECONDS);
            if (record.getStatus() == Status.OK) {
                System.out.printf("[%s] send message success.%n", body);
            } else {
                System.out.printf("[%s] send message failed.%n", body);
            }
        }
        
        producer.shutdown();
    }
    
}
