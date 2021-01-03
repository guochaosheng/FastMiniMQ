# 测试用例

## 测试用例规范
* 原则：可观测，可操作，可复现

* 为了避免实际操作过程中不同测试组的测试结果和结论的出现显著偏差，所有案例至少必须明确如下要点信息：
  1. 源码包
  2. 编译 JDK 和 运行环境 JDK 详细名称和版本（推荐提供对应下载链接）
  3. Maven 版本，非默认配置需要提供相应配置文件
  4. 操作系统名称和版本（推荐提供对应下载链接）
  5. 服务器参数信息，包括机型，CPU型号和核心数，内存大小，网络IO吞吐量，磁盘IOPS和最大吞吐量
  6. 所需部署节点的关系结构图或者拓扑图
  7. 执行命令清单和操作流程说明


## 基准测试清单
| 基准测试清单                                                 | 状态                                                         |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| 异步刷盘，topic 总数 256，消息 body 64字节，1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | [已完](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/benchmark/testcase_async_topic_256_body_64_8c16gx1.md) |
| 异步刷盘，topic 总数 256，消息 body 128字节，1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 256，消息 body 1024字节（1K），1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 256，消息 body 2048字节（2K），1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 256，消息 body 2048字节（4K），1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 1024，消息 body 128字节，1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 1024，消息 body 1024字节（1K），1个 producer 节点，1个 broker 节点（8C16G），1个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 2048，消息 body 128字节，1个 producer 节点，1个 broker 节点（8C16G），4个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 2048，消息 body 1024字节（1K），1个 producer 节点，1个 broker 节点（8C16G），4个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 4096，消息 body 128字节，1个 producer 节点，1个 broker 节点（8C16G），8个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 4096，消息 body 1024字节（1K），1个 producer 节点，1个 broker 节点（8C16G），8个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 10000，消息 body 128字节，1个 producer 节点，1个 broker 节点（8C16G），8个 consumer节点 | 待完                                                         |
| 异步刷盘，topic 总数 10000，消息 body 1024字节（1K），1个 producer 节点，1个 broker 节点（8C16G），8个 consumer节点 | 待完                                                         |
| 同步刷盘，topic 总数 256，消息 body 128字节，1个 producer 节点，1个 broker 节点（4C8G），1个 consumer节点 | 待完                                                         |
| 同步刷盘，topic 总数 256，消息 body 256字节，1个 producer 节点，1个 broker 节点（4C8G），1个 consumer节点 | 待完                                                         |

## 可靠性测试清单
| 基准测试清单              | 状态                                                         |
| :------------------------ | :----------------------------------------------------------- |
| reboot 前后数据一致性验证 | [已完](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/reliability/testcase_reboot.md) |
| kill PID 安全退出前后数据一致性验证 | [已完](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/reliability/testcase_safe_exit.md) |
| 事务消息（不丢失不重复）数据一致性验证 | [已完](https://github.com/guochaosheng/FastMiniMQ/tree/master/docs/test/reliability/testcase_transaction.md) |
| 生产和消费数据一致性验证  | 待完                                                         |
