package cool.spongecaptain.consumerInterceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 消费者的使用案例
 */
public class KafkaConsumerSample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "myConsumer");
        props.put("enable.auto.commit", false);//关闭自动提交，目的是主动提交执行 Consumer 拦截器的 onCommit 方法回调
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 额外配置一下 Kafka Consumer 的拦截器
         */
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,EvenNumberInterceptor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic-of-consumerInterceptor"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            }
            if (!records.isEmpty()){
                consumer.commitAsync((offsets, exception) -> {
                    System.out.println("callback 回调");
                });
            }

        }

    }
    
}
