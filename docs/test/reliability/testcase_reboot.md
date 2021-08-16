# Reliability TestCase (Reboot)

一个 producer 节点（4核8G内存），一个 broker 节点（4核8G内存），一个 consumer 节点（4核8G内存），256 个消息 topic，128 字节消息 body。

## 结构图

![](testcase_reboot.assets/testcase_reboot.svg)
## 测试方案
* 测试项 1：Producer 发送 1000 w 笔消息记录，记录此时 Producer 已得到成功应答消息总数后 Broker 关机，之后启动 Broker 和 Consumer，检查可消费消息记录总数是否同 Producer 已得到成功应答消息总数相等，是则正确，否则错误。

* 测试项 2：Producer 持续发送消息记录，5 分钟后 Broker 关机，记录此时 Producer 已发送消息总数和已得到成功应答消息总数，之后启动 Broker 和 Consumer，检查可消费消息记录总数是否在 Producer 已得到成功应答消息总数和已发送且未失败确认消息总数区间内，是则正确，否则错误。

## 执行命令清单和操作流程
* 测试项 1

| 序号 | 执行命令或者操作                                             | 说明                                                         |
| :--- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| 1    | mvn clean install                                            | 编译打包生成 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar |
| 2    | scp -r FastMiniMQBroker.jar broker@172.31.0.128:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar producer@172.31.0.127:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar consumer@172.31.0.126:/opt | 复制 FastMiniMQBroker.jar 至 broker 节点，复制 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar 至 producer\consumer 节点 |
| 3    | java -Ddata.dir=/data/fastminimq -Dflush.sync=true -XX:+UseG1GC -cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker | 运行 broker（开启同步刷盘）                                  |
| 4    | java -Dip=172.31.0.128 -DmessageCount=10000000 -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ProducerThroughputTest | 运行 producer，发送 1000 w 笔消息记录，记录 Producer 已得到成功应答消息总数 |
| 5    | broker 强制停机 或者 断开电源                                | broker 快速停机                                              |
| 6    | java -Ddata.dir=/data/fastminimq -Dflush.sync=true -XX:+UseG1GC -cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker<br>java -Dip=172.31.0.128 -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ConsumerThroughputTest | 启动运行 broker 和 consumer，检查可消费消息记录总数是否同 Producer 已得到成功应答消息总数相等 |

* 测试项 2

| 序号 | 执行命令或者操作                                             | 说明                                                         |
| :--- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| 1    | mvn clean install                                            | 编译打包生成 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar |
| 2    | scp -r FastMiniMQBroker.jar broker@172.31.0.128:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar producer@172.31.0.127:/opt <br>scp -r FastMiniMQBroker.jar FastMiniMQ-0.13.1-SNAPSHOT-tests.jar consumer@172.31.0.126:/opt | 复制 FastMiniMQBroker.jar 至 broker 节点，复制 FastMiniMQBroker.jar 和 FastMiniMQ-0.13.1-SNAPSHOT-tests.jar 至 producer\consumer 节点 |
| 3    | java -Ddata.dir=/data/fastminimq -Dflush.sync=true -XX:+UseG1GC -cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker | 运行 broker（开启同步刷盘）                                  |
| 4    | java -Dip=172.31.0.128 -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ProducerThroughputTest | 运行 producer，持续发送消息记录，记录 Producer 已发送消息总数和已得到成功应答消息总数 |
| 5    | broker 强制停机 或者 断开电源                                | broker 快速停机                                              |
| 6    | java -Ddata.dir=/data/fastminimq -Dflush.sync=true -XX:+UseG1GC -cp FastMiniMQBroker.jar org.nopasserby.fastminimq.FastMiniMQBroker<br/>java -Dip=172.31.0.128 -cp ./FastMiniMQBroker.jar:./FastMiniMQ-0.13.1-SNAPSHOT-tests.jar org.nopasserby.fastminimq.benchmark.ConsumerThroughputTest | 启动运行 broker 和 consumer，检查可消费消息记录总数是否在 Producer 已得到成功应答消息总数和已发送且未失败确认消息总数区间内 |

## 在阿里云上的测试结果

* 基础信息

1. 服务器参数：

| 规格           | vCPU | 处理器型号                               | 内存（GiB） | 本地存储（GiB）      | 网络基础带宽能力（出/入）（Gbit/s） | 网络突发带宽能力（出/入）（Gbit/s） | 网络收发包能力（出+入）（万PPS） | 连接数（万） | 多队列 | 云盘最大IOPS | 云盘最大吞吐量（MB/s） | 云盘带宽（Gbit/s） |
| :------------- | :--- | :--------------------------------------- | :---------- | :------------------- | :---------------------------------- | :---------------------------------- | :------------------------------- | :----------- | :----- | :----------- | :--------------------- | :----------------- |
| ecs.c6.xlarge  | 4    | Intel Xeon(Cascade Lake) Platinum 8269CY | 8           | 高效云盘 40 G        | 1.5                                 | 5.0                                 | 50                               | 最高25       | 4      | 5000         | 140                    | 1.5                |
| ecs.c6.2xlarge | 4    | Intel Xeon(Cascade Lake) Platinum 8269CY | 8           | 高效云盘 40 G + 1.5T | 1.5                                 | 5.0                                 | 80                               | 最高25       | 4      | 5000         | 140                    | 1.5                |

2. 操作系统：CentOS 7.6 64bit
3. Maven 版本：Apache Maven 3.2.5
4. 编译 JDK 版本：Oracle jdk1.8.0_22164
5. 运行环境 JDK 版本：java-1.8.0-openjdk-1.8.0.252.b09-2.el7_8.x86_64

* FastMiniMQ reboot 可靠性测试报告：
1. 测试项 1（关机方式：控制台强制停止）

| Producer 聚合报告                                            |
| :----------------------------------------------------------- |
| Send Success: 10000000 Response Success: 8031671 Send Failed: 0 Response Failed: 1968329 |
| Producer 发送消息记录总数 10000000，成功确认消息总数 8031671，失败确认消息总数 1968329 |

|Consumer 聚合报告   |
| :------------- |
| Send Success: 5276435 Response Success: 8031671 Send Failed: 0 Response Failed: 0 |
| Consumer 可消费消息记录总数 8031671 |

|结果报告   |
| :------------- |
| 正确，重启后可消费消息记录总数同重启前 Producer 已得到成功应答消息总数相等 |

2. 测试项 2（关机方式：控制台强制停止）

| Producer 聚合报告                                            |
| :----------------------------------------------------------- |
| Send Success: 121805124 Response Success: 96169628 Send Failed: 0 Response Failed: 25521792 |
| Producer 发送消息记录总数 121805124，成功应答消息总数 96169628，失败确认消息总数 25521792 |

|Consumer 聚合报告   |
| :------------- |
| Response Success: 96170415 |
| Consumer 可消费消息记录总数 96170415 |

|结果报告   |
| :------------- |
| 正确，重启后可消费消息记录总数在重启前 Producer 已得到成功应答消息总数和已发送且未失败确认消息总数区间内（96169628 <= 96170415 <= 121805124 - 25521792） |
