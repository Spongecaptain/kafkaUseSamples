# Package 5

本节用于说明 Kafka 中 ConsumerGroup 的使用。

测试方式：
- 启动 group 1 下的两个 Consumer;
- 启动 group 2 下的 Consumer;
- 启动 Producer


由于在测试机上仅仅使用了一个 Kafka broker，因此虽然同一个 group1 中有两个消费者，
也仅仅会有一个消费者能够获取到消息，另一个消费者始终得不到消息。

由于 group1 与 group2 相互独立，因此 group2 中的消费者将独立得到所有的消息。



