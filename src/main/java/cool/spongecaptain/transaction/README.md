# README

用于测试 Kafka 的 consume-transform-produce 过程中的事务 API。

启动逻辑：

- 先启动 ConsumeTransformProduce
- 再启动 ConsumerFromSinkTopic
- 最后启动 ProducerForSourceTopic

我们可以在最后的 topic-sink 主题上不断地消费到经过流处理之后的消息，例如：

offset = 117, key = SpongecaptainKey5 transformed, value = SpongecaptainValue5 transformed


