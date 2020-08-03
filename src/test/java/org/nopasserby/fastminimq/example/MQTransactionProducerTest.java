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

import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQRegistry;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class MQTransactionProducerTest {
    
    public static void main(String[] args) throws Exception {
        MQClusterMetaData clusterMetaData = MQRegistry.loadClusterMetaData("cluster-test::broker-test@127.0.0.1:6001;");
        
        MQProducerCfg producerCfg = new MQProducerCfg("producer-test", "cluster-test");
        MQProducer producer = new MQProducer(producerCfg, clusterMetaData);
        producer.start();
        
        String topic = "testTopic";
        int batchCount = 1000;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        
        for (int i = 0; i < batchCount; i++) {
            String body = "message no:" + i + ", datetime:" + sdf.format(new Date());
            MQFuture<MQRecord> future = producer.sendTxMsg(topic, body.getBytes());
            MQRecord record = future.get();
            producer.commit(record).get();
            System.out.printf("%s%n", body);
        }
        
        producer.shutdown();
    }

}
