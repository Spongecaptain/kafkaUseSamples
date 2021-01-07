# Package 3

本节用于说明 Kafka 的 Producer 端的 Interceptor 的简单实例案例。

分为两步：

1.实现 PrefixInterceptor 类
2.额外写入配置：
```java
props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,PrefixInterceptor.class.getName());
```