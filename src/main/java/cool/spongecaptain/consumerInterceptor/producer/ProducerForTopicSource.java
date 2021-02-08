package cool.spongecaptain.consumerInterceptor.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 为 Topic 为 topic-source 的生产消息
 */
public class ProducerForTopicSource {

    public static final String TOPIC_SOURCE = "topic-source";
    public static final String BROKER_LIST = "localhost:9092";

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
    //为了模拟事务，我们先需要为 topic-source 主题写入一些消息
    public void startProduce() {
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(getProducerProperties());
        //生产 10 个消息即可
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_SOURCE, "SpongecaptainKey" + i, "SpongecaptainValue" + i);
            producer.send(record);
        }
        producer.close();

    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(getProducerProperties());
        //生产 10 个消息即可
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_SOURCE, "SpongecaptainKey" + i, "SpongecaptainValue" + i);
            producer.send(record);
        }
        producer.close();
    }
}
