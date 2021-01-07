package cool.spongecaptain.serial.consumer;

import cool.spongecaptain.serial.deserializer.CatDeserializer;
import cool.spongecaptain.serial.dto.Cat;
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
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        /**
         * 仅仅 value 是 Cat 类型
         */
        props.put("value.deserializer", CatDeserializer.class.getName());


        KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic-of-codec"));
        while (true) {
            ConsumerRecords<String, Cat> records = consumer.poll(100);
            for (ConsumerRecord<String, Cat> record : records)
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
        }
    }
    
}
