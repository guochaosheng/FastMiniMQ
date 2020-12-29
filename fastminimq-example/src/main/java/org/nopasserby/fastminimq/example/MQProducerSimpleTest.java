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

import java.util.concurrent.TimeUnit;

import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQProducer;
import org.nopasserby.fastminimq.MQRegistry;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.nopasserby.fastminimq.MQProducer.MQProducerCfg;

public class MQProducerSimpleTest {

    public static void main(String[] args) throws Exception {
        // 创建 Broker 集群元数据对象（集群名称::消息服务节点名称@节点地址;消息服务节点名称@节点地址;...）
        MQClusterMetaData clusterMetaData = MQRegistry.loadClusterMetaData("cluster-test::broker-test@127.0.0.1:6001;");
        // 创建 Producer 配置对象（生产者名称，连接集群名称）
        MQProducerCfg producerCfg = new MQProducerCfg("producer-test", "cluster-test");
        MQProducer producer = new MQProducer(producerCfg, clusterMetaData);
        // 启动 Producer
        producer.start();

        String topic = "testTopic";
        String body = "hello world!";
        // 发送主题消息
        MQFuture<MQRecord> future = producer.sendMsg(topic, body.getBytes());
        // 设置 30 秒等待
        MQRecord record = future.get(30, TimeUnit.SECONDS);
        if (record.getStatus() == Status.OK) {
            System.out.printf("[%s] send message success.%n", body);
        } else {
            System.out.printf("[%s] send message failed.%n", body);
        }

        // 关闭 Producer
        producer.shutdown();
    }
    
}
