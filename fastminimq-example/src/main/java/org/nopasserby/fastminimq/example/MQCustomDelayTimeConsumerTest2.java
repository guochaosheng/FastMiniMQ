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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.nopasserby.fastminimq.MQConsumer;
import org.nopasserby.fastminimq.MQQueue;
import org.nopasserby.fastminimq.MQRegistry;
import org.nopasserby.fastminimq.MQResult;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;

public class MQCustomDelayTimeConsumerTest2 {

    public static void main(String[] args) throws Exception {
        MQClusterMetaData clusterMetaData = MQRegistry.loadClusterMetaData("cluster-test::broker-test@127.0.0.1:6001;");
        
        MQConsumerCfg consumerCfg = new MQConsumerCfg("consumer-test", "cluster-test", "broker-test");
        MQConsumer consumer = new MQConsumer(consumerCfg, clusterMetaData);
        consumer.start();
        
        String topic = "testTopic";
        MQQueue queue = createMQQueue(topic, MQGroup.GROUP_1);
        
        int batchCount = 1000;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        long delayTime = TimeUnit.MINUTES.toMillis(3);
        while (batchCount > 0) {
            MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(queue);
            MQResult<List<MQRecord>> result = future.get();
            List<MQRecord> recordList = result.getResult();
            for (MQRecord record: recordList) {
                // Wait for a fixed delay time
                long intervalTime = System.currentTimeMillis() - record.getTimestamp();
                if (intervalTime < delayTime) Thread.sleep(delayTime - intervalTime);
                
                System.out.printf("%s,%s,%d %n", sdf.format(new Date()), new String(record.getBody()), System.currentTimeMillis() - record.getTimestamp());
                batchCount--;
            }
            queue.ack();
        }
        
        consumer.shutdown();
    }
    
    public static MQQueue createMQQueue(String topic, MQGroup group) {
        MQQueue queue = new MQQueue();
        MQGroup[] groups = MQGroup.values();
        queue.setGroup(MQGroup.class.getSimpleName());
        queue.setTopic(topic);
        queue.setSubgroups(groups.length);
        queue.setSubgroupNo(group.ordinal());
        queue.setIndex(0);
        queue.setStep(400);
        return queue;
    }
    
    enum MQGroup {
        GROUP_1
    }

}
