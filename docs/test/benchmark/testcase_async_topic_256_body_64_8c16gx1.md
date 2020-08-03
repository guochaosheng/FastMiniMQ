# Benchmarks TestCase (256 Topic, 64 Bytes Body)
一个 producer 节点（4核8G内存），一个 broker 节点（8核16G内存），一个 consumer 节点（4核8G内存），256 个消息 topic，64 字节消息 body。

## 部署结构图

![](https://www.guochaosheng.com/fastminimq/docs/img/fastminimq_pref_deploy.svg)

## 执行命令清单和操作流程

| 序号 | 执行命令或者操作                                             | 说明                                                         |
| :--- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| 1    | mvn clean install                                            | 编译打包生成 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar |
| 2    | scp -r FastMiniMQBroker.jar broker@172.31.0.128:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar producer@172.31.0.127:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar consumer@172.31.0.126:/opt | 复制 FastMiniMQBroker.jar 至 broker 节点，复制 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar 至 producer\consumer 节点 |
| 3    | nohup java -Ddata.dir=/data/fastminimq -Xmx12G -Xms12G -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:broker.gc.log -cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker > broker.log & | 运行 broker（打印 gc 详细日志）                              |
| 4    | java -Dip=172.31.0.128 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:consumer.gc.log -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ConsumerThroughputTest | 运行 consumer（打印 gc 详细日志）                            |
| 5    | java -Dip=172.31.0.128 -DmessageSize=64 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:producer.gc.log -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ProducerThroughputTest | 运行 producer（打印 gc 详细日志）                            |

## 在阿里云上的测试结果

* 基础信息

1. 服务器参数：

| 规格           | vCPU | 处理器型号                               | 内存（GiB） | 本地存储（GiB）      | 网络基础带宽能力（出/入）（Gbit/s） | 网络突发带宽能力（出/入）（Gbit/s） | 网络收发包能力（出+入）（万PPS） | 连接数（万） | 多队列 | 云盘最大IOPS | 云盘最大吞吐量（MB/s） | 云盘带宽（Gbit/s） |
| :------------- | :--- | :--------------------------------------- | :---------- | :------------------- | :---------------------------------- | :---------------------------------- | :------------------------------- | :----------- | :----- | :----------- | :--------------------- | :----------------- |
| ecs.c6.xlarge  | 4    | Intel Xeon(Cascade Lake) Platinum 8269CY | 8           | 高效云盘 40 G        | 1.5                                 | 5.0                                 | 50                               | 最高25       | 4      | 5000         | 140                    | 1.5                |
| ecs.c6.2xlarge | 8    | Intel Xeon(Cascade Lake) Platinum 8269CY | 16          | 高效云盘 40 G + 1.5T | 2.5                                 | 8.0                                 | 80                               | 最高25       | 8      | 5000         | 140                    | 2                  |

2. 操作系统：CentOS 7.6 64bit

3. Maven 版本：Apache Maven 3.2.5

4. 编译 JDK 版本：Oracle jdk1.8.0_22164

5. 运行环境 JDK 版本：java-1.8.0-openjdk-1.8.0.252.b09-2.el7_8.x86_64

* FastMiniMQ 基准测试报告：

1. 磁盘IO读写 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_disk_read_write_bytes(Bps).png)

2. 网络IO进出 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_net_in_out_rate(bps).png)

3. CPU消耗 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_cpu_used_rate.png)

4. 内存占用 [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_mem_used_total.png)

5. 磁盘 IOPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_disk_read_write_requests.png)

6. GC Duration Time [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/gc_duration_time.png)

7. GC Causes [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/gc_causes.png)

8. GC Statistics [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/gc_statistics.png)

9. Producer Statistics TPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_producer_statistics_tps.png)

10. Consumer Statistics TPS [时段 2020-08-02 12:45:00 - 2020-08-02 15:50:00]

![](https://www.guochaosheng.com/fastminimq/docs/img/monitor_consumer_statistics_tps.png)

更多测试报告看：[fastminimq-testcase-list](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/testcase_list.md)