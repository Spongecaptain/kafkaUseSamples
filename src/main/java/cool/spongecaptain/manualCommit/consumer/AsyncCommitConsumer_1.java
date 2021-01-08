package cool.spongecaptain.manualCommit.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 消费者进行异步式地提交
 */
public class AsyncCommitConsumer_1 {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "AsyncCommitConsumer_1");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 将 auto-commit 关闭
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic-of-commit"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            }

            //处理完一轮消息后，进行一步地提交。存在消息重复消费问题：比如消息处理后，KafkaConsumer 马上宕机，还没来得及 commit

            //首先进行检查，是否有必要进行提交，标准是此轮 records 不为空，否则完全没有必要进行提交
            if(!records.isEmpty()){
                consumer.commitAsync(((offsets, exception) -> offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                    //没有发生异常
                    if(exception==null){
                        //因为在单机下运行 Kafka 服务端，所以事实上只有一个 topicPartition
                        System.out.println("异步提交的 offset 为 "+offsetAndMetadata.offset());
                    }else{
                        System.out.println("异步 offset 提交出错");
                    }
                }))));
            }
        }
    }
    
}
