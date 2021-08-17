# FastMiniMQ

[![Github Build Status](https://github.com/guochaosheng/FastMiniMQ/workflows/CI/badge.svg?branch=master)](https://github.com/guochaosheng/FastMiniMQ/actions)  [![Coverage Status](https://coveralls.io/repos/github/guochaosheng/FastMiniMQ/badge.svg?branch=master)](https://coveralls.io/github/guochaosheng/FastMiniMQ?branch=master)  [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

轻量，高效。FastMiniMQ 是一个消息队列。非常轻，源代码大小 200 ~ 300 KB，运行程序包大小约 5 M。非常快，8 核 16 G（磁盘最大吞吐量 140 MB/s，最大 IOPS 5000）单机消息生产和消费均可稳定维持 100 万 TPS（[看看我们是怎么做的](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/design/fastminimq_design_analysis.md)）。
## API Example 
* MQProducer Send Message Example
```
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
```
* MQConsumer Fetch Message Example
```
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
```
更多示例看：[fastminimq-examples](https://github.com/guochaosheng/FastMiniMQ/tree/master/src/test/java/org/nopasserby/fastminimq/example)

## Requirements
* Java 8+
* slf4j library
* netty library

## Benchmarks 
一个 producer 节点（4核8G内存），一个 broker 节点（8核16G内存），一个 consumer 节点（4核8G内存），256 个消息 topic，64 字节消息 body。

* 结构图

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/fastminimq_pref_deploy.svg)

* 构建

```
mvn clean install
```
* 运行 broker（打印 gc 详细日志）

```
nohup java -Ddata.dir=/data/fastminimq \
-Xmx12G -Xms12G -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:broker.gc.log \
-cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker > broker.log &
```
* 运行 consumer（打印 gc 详细日志）

```
java -Dip=172.31.0.128 \
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:consumer.gc.log \
-cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ConsumerThroughputTest
```
* 运行 producer（打印 gc 详细日志）

```
java -Dip=172.31.0.128 -DmessageSize=64 \
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:producer.gc.log \
-cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ProducerThroughputTest
```
* 在阿里云上的测试结果

服务器参数：

| 规格           | vCPU | 处理器型号                               | 内存（GiB） | 本地存储（GiB）      | 网络基础带宽能力（出/入）（Gbit/s） | 网络突发带宽能力（出/入）（Gbit/s） | 网络收发包能力（出+入）（万PPS） | 连接数（万） | 多队列 | 云盘最大IOPS | 云盘最大吞吐量（MB/s） | 云盘带宽（Gbit/s） |
| :------------- | :--- | :--------------------------------------- | :---------- | :------------------- | :---------------------------------- | :---------------------------------- | :------------------------------- | :----------- | :----- | :----------- | :--------------------- | :----------------- |
| ecs.c6.xlarge  | 4    | Intel Xeon(Cascade Lake) Platinum 8269CY | 8           | 高效云盘 40 G        | 1.5                                 | 5.0                                 | 50                               | 最高25       | 4      | 5000         | 140                    | 1.5                |
| ecs.c6.2xlarge | 8    | Intel Xeon(Cascade Lake) Platinum 8269CY | 16          | 高效云盘 40 G + 1.5T | 2.5                                 | 8.0                                 | 80                               | 最高25       | 8      | 5000         | 140                    | 2                  |

操作系统：CentOS 7.6 64bit

Maven 版本：Apache Maven 3.2.5

编译 JDK 版本：Oracle jdk1.8.0_22164

运行环境 JDK 版本：java-1.8.0-openjdk-1.8.0.252.b09-2.el7_8.x86_64

FastMiniMQ 基准测试报告：

磁盘 IO 读写 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_disk_read_write_bytes(Bps).png)

网络 IO 进出 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_net_in_out_rate(bps).png)

CPU 消耗 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_cpu_used_rate.png)

内存占用 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_mem_used_total.png)

磁盘 IOPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_disk_read_write_requests.png)

GC Duration Time [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/gc_duration_time.png)

GC Causes [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/gc_causes.png)

GC Statistics [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/gc_statistics.png)

Producer Statistics TPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_producer_statistics_tps.png)

Consumer Statistics TPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://raw.githubusercontent.com/guochaosheng/FastMiniMQ/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.assets/monitor_consumer_statistics_tps.png)

更多测试报告看：[fastminimq-testcase-list](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/testcase_list.md)

## Features
1. 支持从指定时间开始消费
2. 支持定时延迟消息
3. 支持事务消息

## Todo 
1. 集群管理
2. Raft 多副本
3. 自动扩容和负载均衡

## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Guo Chaosheng