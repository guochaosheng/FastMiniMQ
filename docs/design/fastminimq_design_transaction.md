# FastMiniMQ 消息事务机制设计

### 前言

​		消息事务是 FastMiniMQ 的核心功能之一，FastMiniMQ 的目标是提供一套低代价、高性能、高可靠的 MQ 消息事务机制，这里将详细阐述 FastMiniMQ 消息事务机制设计和验证流程。



### MQ 事务消息

在 [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)协议中，消息服务质量等级如下，

* QoS 0: 至多分发一次（at most once）

  消息至少送达一次，消息可能送达一次也可能根本没送达。

* QoS 1: 至少分发一次（at least once）

  消息至少送达一次，可能重复不会丢失。

* QoS 2: 仅分发一次（exactly once）

  消息仅且送达一次，这是最高等级的服务质量，消息不丢失不重复。

​       注意，消息是否送达是由消费者决定的，只有消费者确认后才算送达，如果消费者在消费处理期间宕机，没有确认接收到消息，则不视为送达。如果没有特别说明，下面所有提及的 MQ 事务消息都是指 QoS 2（Exactly Once ）类型的消息。



### MQ 事务消息验证模型

​		MQ 事务机制确保消息的生产和消费操作要么都成功，要么都失败。



<div align=center><img width="85%" src="fastminimq_design_transaction.assets/MQ 事务消息验证模型.svg"/></div>

​		DB 作为操作记录者，每当生产者提交消息返回成功时就插入一条消息 ID 记录，每当消费者取得消息时也插入一条消息 ID 记录，MQ 事务机制保证生产者和消费者两端的 DB 所记录生产和消费消息必定在某个时间达成一致（最终一致）。当生产者停止生产，消费者消费结束，DB 所记录生产和消费全量消息也必定相同。



### MQ 事务消息实现的基本问题

​		MQ 消息在生产和消费过程中可能发生两种故障：网络断线和停机。MQ 事务机制确保任意时刻任何节点发生了网络断线和停机的前提下，一个事务消息的生产和消费操作最终要么都成功，要么都失败。



保证 MQ 事务消息正确性主要有如下 3 个层面的问题

* 单节点单会话

  单节点：只有一个生产者，一个消息中间件，一个消费者。

  单会话：生产者和消息中间件，消息中间件和消费者二者会话自始至终只有一个，不断线，不停机。

  假如不存在断线和停机故障，用 TCP 这种可靠传输协议足以保证生产者、消息中间件、消费者节点间消息分发不丢失不重复。

* 单节点多会话

  单节点：只有一个生产者，一个消息中间件，一个消费者。

  多会话：生产者、消息中间件、消费者可能发生网络断线和停机，因此需多个连接会话来完成一个消息的生产或者消费操作。

  网络断线带来的问题：当生产者向消息中间件提交一个消息，此时发生网络故障，生产者和消息中间件之间的连接会话被断开，此时，消息中间件有可能已处理消息，也有可能没有收到消息，MQ 事务机制需确保后续处理中，这个消息的生产和消费操作结果状态达成一致（要么都成功，要么都失败）。

  停机带来的问题：停机不仅会导致网络断线，也导致非持久化消息丢失。

* 多节点多会话

  多节点：有多个生产者，多个消息中间件，多个消费者。
  
  多会话：同上。
  
  多节点多会话相比单节点多会话更多问题主要在于故障转移上。
  
  第一，生产者在提交普通消息时如果目标消息中间件响应超时，生产者往往会做自动重试和故障转移，选择其它消息中间件节点重新提交消息，这种自动重试和故障转移策略提高了系统可用性，但是对于事务消息，故障转移很容易导致一个事务消息重复消费，例如，有一个生产者和两个消息中间件，生产者和消息中间件之间用 MQTT QoS 2 分发协议，按照协议消息提交分两阶段，在第一阶段生产者向消息中间件 A 提交消息时，消息中间件 A 收到并存储消息后网络断线，生产者由于没有收到响应会一直等待直到超时后发起自动重试和故障转移，选择另一个消息中间件 B 提交消息，消息中间件 B 收到并存储后成功响应，生产者收到成功响应后继续向消息中间件 B 提交第二阶段消息，消息中间件 B 收到后在准备响应时也发生网络断线，生产者由于没有收到第二阶段消息提交响应，在等待超时后发起自动重试和故障转移，重新选择已恢复连接的消息中间件 A 并向其提交第二阶段消息，消息中间件 A 收到后成功响应，生产者收到成功响应后删除其存储消息，这种情况下消息中间件 A 和 消息中间件 B 会同时持有同一个事务消息，如果两个中间件各自连接不同的消费者，同一个消息就可能被不同消费者消费。
  
  第二，消费者在取得消息并做消费处理期间，如果发生掉线或者宕机，期间这部分消息有可能未消费，也可能已消费，在没有明确得到这部分消息的消费结果状态前，其它消费者都不能处理这些消息，如果此时因为消费者节点宕机而把这些未确认已消费（可能未消费，也可能已消费）的消息转移给其它消费者节点做消费处理，就可能导致事务消息重复消费。
  



### FastMiniMQ 事务机制

​		在 FastMiniMQ 中事务消息有 2 种模式可选：消息中间件端去重模式和消费端去重模式。



* 消息代理端去重模式

<div align=center><img width="100%" src="fastminimq_design_transaction.assets/FastMiniMQ 事务机制-消息代理端去重模式.svg"/></div>

​		上述流程中，生产者操作步骤为 P1 至 P7，消费者操作步骤为 S1 至 S3，注意消费者更新消费队列操作必须和消息消费操作在同一个事务中，二者必须同时成功或者失败，否则可能导致消息重复消费。

 * 消费端去重模式


<div align=center><img width="100%" src="fastminimq_design_transaction.assets/FastMiniMQ 事务机制-消费端去重模式.svg"/></div>

​		上述流程中，生产者操作步骤为 P1 至 P7，消费者操作步骤为 S1 至 S7。注意消费者删除预备消息操作必须和消息消费操作在同一个事务中，二者必须同时成功或者失败，否则可能导致消息重复消费。

​		上面的两种事务消息模式中，生产者 P1 操作步骤可以合并到 P4 操作步骤，等发送预备消息操作返回后再将消息和消息状态存储到本地库，这样可以减少 1 次本地事务，但是在使用消息代理端去重而不是消费端去重模式时可能会导致消息中间件存在非常少量废弃的预备消息，因为生产者如果宕机而没把发送的消息和消息状态存储到本地库，没有消息记录重启后消息也无从提交或回滚，这些废弃预备消息会等到日志过期（默认保留 7 天）后才一起删除。

​       上面的两种事务消息模式都是基于消费操作无幂等性的前提，如果消费操作具有幂等性，那么只需要保证 QoS 1 服务质量等级，由此只需要在普通消息投递流程上增加消息补偿机制确保消息至少送达一次，其流程如下

<div align=center><img width="80%" src="fastminimq_design_transaction.assets/FastMiniMQ 消息补偿机制.svg"/></div>

​		上述流程中，生产者操作步骤为 P1 至 P4，消费者操作步骤为 S1 至 S2。需特别注意的是，生产端消息尝试发送次数需配置为 1（默认是 3）。原因在于默认情况下，生产者向消息中间件集群发送普通消息时会选择其中一个尝试发送，如果消息发送失败就会自动进行故障转移，从集群选择另其中一个重新发送，由于多次尝试向不同消息中间件发送同一个消息可能导致不同消息中间件持有同一个消息，如果每个消息中间件各自连接不同的消费者，此时同一个消息就可能被不同消费者消费。



### FastMiniMQ 事务机制验证流程

​	集群节点故障主要有如下 3 个层面

* 单节点故障：网络掉线、人为强制关机重启、服务器 CPU 故障、服务器主板故障等
* 多节点同时故障：交换机故障、电源插座故障等
* 全部节点同时故障：UPS 故障，机房空调故障等

​    上述故障都是可以自动或者人工恢复的，MQ 事务机制需要保证在上述任意故障中，任意一个事务消息的生产操作和消费操作最终要么都成功，要么都失败。FastMiniMQ 事务机制验证流程如下



<div align=center><img width="75%" src="fastminimq_design_transaction.assets/FastMiniMQ 事务机制验证流程-节点部署拓扑结构图.svg"/></div>

​		上述拓扑结构图中，Producer-1、Producer-2、Producer-3、Producer-4 是生产者节点，Consumer-1、Consumer-2、Consumer-3、Consumer-4 是消费者节点，Broker-1、Broker-2 是消息中间件节点。

​		Producer 侧节点持续生产事务消息，Consumer 侧节点持续消费事务消息，每间隔 3 分钟 Chaos Monkey 在其余 10 节点中随机选择部分或者全部节点强制重启，也就是说单次强制重启节点数在 1 至 10 区间范围内，Chaos Monkey 确保 1-10 每种情况至少包含一次。Chaos Monkey 通过 SSH 指令实现对节点的远程操作，远程节点执行强制重启后 30 秒节点应用再重新启动。Chaos Monkey 持续 300 分钟（节点数 10 的平方乘以时间间隔 3 分钟）后停止强制重启操作，启动数据校验进程，数据校验进程等待 Consumer 消费结束后开始检查 Producer 侧已完成投递事务消息和 Consumer 侧已消费处理消息是否一致。

校验生产和消费消息是否一致，分成如下 2 种情况：

1. 当 Producer 侧存在已开始第二阶段投递但未确认投递成功消息，此时不同状态消息数量满足如下关系

* Producer 侧已完成提交事务消息总数量 <= Consumer 侧已消费处理消息总数量

* Producer 侧已完成提交事务消息总数量 + 已开始第二阶段投递但未确认投递成功消息总数量 >= Consumer 侧已消费处理消息总数量

2. 当 Producer 侧全部事务消息投递成功（或者不存在已开始第二阶段投递但未确认投递成功消息），此时不同状态消息数量满足如下关系

* Producer 侧已完成提交事务消息总数量 = Consumer 侧已消费处理消息总数量



### FastMiniMQ 事务机制答疑



问题 1. FastMiniMQ 事务机制需要 Producer 和 Consumer 本地事务支持，这成本是否过高？

> FastMiniMQ 事务基于下面前提  
> \1. 任意节点在任意时刻都有可能停机。
> \2. 任意两个节点间的连接在任意时刻都有可能中断。
>
>
> 导致强制停机或者连接意外中断原因有可能是硬件设备故障，也可能是人为强制关机、重启等。 

想要确保严格意义的 Exactly Once，在符合上述前提下实现 Exactly Once，就必须要有本地事务，任何 MQ 都无法避免，这个严格证明的。

对于 Producer 和 Broker，Producer 向 Broker 发送一个消息，假设这时候出现硬件故障 Broker 和 Producer 先后宕机，那么 Producer 和 Broker 重启后，这个消息的状态是完全未知的，这个消息后续该如何处理？Broker 有可能根本没收到这个消息，假如 Broker 没有收到这个消息，那么 Broker 肯定无法去回查确认这个消息的状态，这时候只能由 Producer 去回查这个消息的状态，因此 Producer 必须有本地事务支持。当然，有些系统的 Producer 消息来自于其它系统并且这些外部系统具有存储和重试消息功能，亦或者交由用户自己去回查单据，Producer 只是充当中间转发器，这种情况不属于谈论范畴。MQ 事务机制是把 Producer 单个节点及其依赖系统的集合当做一个 Producer 端点看待。

对于 Broker 和 Consumer，情况同 Producer 和 Broker。当 Consumer 在消费一个消息过程中宕机，想要确保严格意义的 Exactly Once，这个消息只能被宕机服务器独占，其它任意 Consumer 节点都不能处理该消息，否则就可能会出现重复消费。因此，Consumer 也必需有本地事务支持用来存储消息消费状态。FastMiniMQ 支持把消费队列状态保存到 Broker，但是想要确保严格意义的 Exactly Once，就必须把状态和消费处理操作绑定到同一个事务中。FastMiniMQ 目前的 Consumer 消息消费负载均衡只支持手动配置其中一个原因在于，FastMiniMQ 期望确保严格意义的 Exactly Once ，当某个 Consumer 节点宕机或者掉线时，系统必须确保其它 Consumer 节点不会消费处理 Consumer 宕机或者掉线前正在处理且消费状态不明确的消息，如果此时把未明确状态的消费队列移交给其它 Consumer 节点处理就可能会造成重复消费。

MQ 事务机制的数据一致性问题最终依然归结于分布式事务的“两将军问题”和“八皇后问题”。



问题 2. FastMiniMQ 事务机制和 MQTT 事务机制有些相似，其特点是什么？

FastMiniMQ 事务机制不是由 MQTT 事务机制衍生而来，FastMiniMQ 之所以选择用这种方式，是因为 FastMiniMQ 发现这种方式实现简单，而且非常可靠，其实现符合严格意义的 Exactly Once。FastMiniMQ 事务机制中 Producer 和 Broker 间的两阶段消息提交和 MQTT 事务机制中的基本相同，有些 MQ 事务机制在第二阶段处理时采用由 Broker 回调 Producer 方式。FastMiniMQ 事务机制设计不用回调方式只要出于下面 2 个原因，

1. Producer 端需要在确认事务消息已提交后支持及时删除消息，防止消息及其相关记录长时间堆积，如果用回调，Producer 端无法确认事务消息是否已提交，Broker 有可能在回调后的处理过程中出问题，因此可能多次回调，Producer 在遇到回调时不便删除消息及其相关记录。想要支持删除消息，回调方式下要增加一个通知，二者合计至少就需要二次回调。
2. 不利于兼容 HTTP 这种类型的短连接协议。

FastMiniMQ 事务机制在消息去重上有 2 种方式，其一在 Broker 端去重，其二在 Consumer 端去重。下面是去重逻辑代码实现。

```java
if (sign == PRE_TX) {
	globalIdIndex.add(globalId, recordOffset + recordDataLength); // only the last message is valid
	return true;
}

if (sign == COMMIT_TX || sign == ROLLBACK_TX) {
	if (!globalIdIndex.remove(globalId)) {
	// skip duplicate 
	return true;
	}
}
```

用在 Broker 端去重需要谨慎保障充足内存空间，在 FastMiniMQ 目前版本实现中 Broker 并不允许存在过多半消息，因此 Producer 端应该及时 Commit/Rollback。之所以称 FastMiniMQ 事务机制简单，主要在 Consumer 端去重方式上，只要支持 ACK 确认的 MQ，即便不支持事务机制 MQ 也可以很轻易做到，因为这时候 Broker 端不需要做任何处理，Consumer 端按顺序读取消费队列，结合本地事务按照上述去重逻辑处理即可。如果是 Prepare Message，就保留最后一个，如果是 Commit/Rollback Message，就通过 Message ID 本地回查 Prepare Message ，如果存在则事务消息投递成功，删除记录，如果不存在说明要么是非法操作，要么是消息已经接收过，直接忽略即可。

Producer 端第一个阶段投递的目的实际上就是把用于去重标记（ Message ID）投递给 Broker/Consumer 端。在 FastMiniMQ 中 Producer 端投递时确保有效事务消息不会因设备通信故障重发到不同的 Broker 节点，其目的是避免单个 Consumer 节点做消息去重时需要查询所有 Consumer 节点的消费记录，那样会导致 Consumer 节点需要引入外部节点依赖，比如引入 Redis 中心去做去重。

在 FastMiniMQ 中选择在 Consumer 端去重需要注意，同一队列的多个虚拟队列不能分配给多个不同 Consumer 节点。如果单个 Consumer 负载过大，一种简单的处理办法是把单个主题拆分成多个主题，比如主题 Topic-1 拆分成 4 个主题 Topic-1-Queue-1、Topic-1-Queue-2、Topic-1-Queue-3、Topic-1-Queue-4，然后可以用 4个节点 Consumer 节点分别订阅这 4 个主题。
