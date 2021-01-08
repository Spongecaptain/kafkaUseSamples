package cool.spongecaptain.consumerGroupTest.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Kafka 生产者的使用案例
 */
public class KafkaProducerSample {
    public static void main(String[] args) {
        //这里使用 java.util.Properties 类来存储一系列属性，用于构造 org.apache.kafka.clients.producer.Producer 接口实例
        //通过我们是将配置文件解析为 Properties 实例，而不会选择构造 Properties 实例，然后通过 put 方法进行初始化
        Properties props = new Properties();
        //配置本地 Kafka 的 IP 地址
        props.put("bootstrap.servers", "localhost:9092");
        //ack是判别请求是否为完整的条件（就是是判断是不是成功发送了）。我们指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的。
        props.put("acks", "all");
        //retries，如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。
        props.put("retries", 0);
        //指定缓冲区发中每一个批的大小为 2^15 大小
        props.put("batch.size", 16384);
        //用于配置缓存在时间维度的发送逻辑
        props.put("linger.ms", 1);
        //用于配置缓存的最大大小为 2^25 大小
        props.put("buffer.memory", 33554432);
        //指定 key 以及 value 各自的序列化器，这里自然是将 Java 中的 String 类型实例转换为字节的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //构造具体类型为 org.apache.kafka.clients.producer.KafkaProducer 类型的生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);

        for(int i = 0; i < 10; i++) {
            //Producer#send 方法用于异步发送消息，这里的每一个消息都为键值对，并存放于 my-topic 主题下
            producer.send(new ProducerRecord<>("topic-of-consumerGroup", "SpongecaptainKey" + i, "SpongecaptainValue" + i));
        }
        //必须要记得关闭资源，当然，最好的方式一定是利用 try-with-resources 来自动完成这个过程
        producer.close();
    }
}
