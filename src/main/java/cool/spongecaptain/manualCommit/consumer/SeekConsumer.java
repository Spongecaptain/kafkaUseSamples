package cool.spongecaptain.manualCommit.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * 这是一个使用 KafkaConsumer#seek 操作的 Consumer
 */
public class SeekConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "SeekConsumer");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 将 auto-commit 打开
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("topic-of-commit"));

        int size = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                /**
                 * 当消息累加到 10 个后进行回退到 offset 为 0 处
                 * 因此，这会导致整个 main 方法在 while 循环内反复消费 offset 为 0-9 范围内的消息
                 */
                if(++size>=10){
                    size =0;
                    consumer.seek((TopicPartition)records.partitions().toArray()[0],0L);
                    break;
                }
            }
        }
    }
}
