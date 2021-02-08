package cool.spongecaptain.transaction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * Kafka 的
 * consume - transform - produce 流式处理模型
 */
public class ConsumeTransformProduce {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String TOPIC_SOURCE = "topic-source";
    public static final String TOPIC_SINK = "topic-sink";
    public static final String CONSUMER_GROUP_ID = "groupOfConsumerTransformProduce";

    public static Properties getConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        //消息的 key 与 value 都应当为 String 类型
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //禁止消费端自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        return props;
    }

    public static Properties getProducerProperties(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "123321");
        return props;
    }

    //consumer - transform -produce 模式示例
    public static void main(String[] args) {
        //1. 初始化消费端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(TOPIC_SOURCE));
        //2. 初始化 Producer 端
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(getProducerProperties());
        //3. 初始化事务
        producer.initTransactions();
        while (true) {
            //4. 消费者拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                //5. 生产者开启事务
                producer.beginTransaction();
                //6. 消费-生产模型
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            //do some logical processing.
                            System.out.println(record.key());
                            System.out.println(record.value());
                            ProducerRecord<String, String> producerRecord =
                                    new ProducerRecord<>(TOPIC_SINK, record.key()+" transformed", record.value()+" transformed");
                            producer.send(producerRecord);
                        }
                        long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }
                    //7. 提交消费位移
                    producer.sendOffsetsToTransaction(offsets,CONSUMER_GROUP_ID);
                    //8. 生产者提交事务
                    producer.commitTransaction();
                } catch (ProducerFencedException e) {
                    //log the exception
                    //9. 生产者中止事务
                    producer.abortTransaction();
                }
            }else{
                System.out.println("empty");
            }
        }
    }
}
