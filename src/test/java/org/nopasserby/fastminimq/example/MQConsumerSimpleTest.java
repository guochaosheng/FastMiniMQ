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

import java.util.List;

import org.nopasserby.fastminimq.MQRegistry;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.nopasserby.fastminimq.MQConsumer;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQQueue;

public class MQConsumerSimpleTest {

    public static void main(String[] args) throws Exception {
        // 创建 Broker 集群元数据对象（集群名称::消息服务节点名称@节点地址;消息服务节点名称@节点地址;...）
        MQClusterMetaData clusterMetaData = MQRegistry.loadClusterMetaData("cluster-test::broker-test@127.0.0.1:6001;");
        // 创建 Producer 配置对象（消费者名称，连接集群名称，连接消息服务节点名称）
        MQConsumerCfg consumerCfg = new MQConsumerCfg("consumer-test", "cluster-test", "broker-test");
        MQConsumer consumer = new MQConsumer(consumerCfg, clusterMetaData);
        // 启动 consumer
        consumer.start();

        // 创建本地消费者分组队列
        MQQueue queue = new MQQueue("testTopic", "testConsumeGroup");
        // 更新最新队列信息
        consumer.fetchUpdate(queue);

        // 请求获取消息记录
        MQFuture<MQResult<List<MQRecord>>> future = consumer.fetchMsg(queue);
        MQResult<List<MQRecord>> result = future.get();
        List<MQRecord> recordList = result.getResult();
        for (MQRecord record: recordList) {
            System.out.printf("%s,%s%n", record.getTopic(), new String(record.getBody()));
        }
        // 确认消费
        consumer.waitAck(queue);

        // 关闭 consumer
        consumer.shutdown();
    }
    
}
