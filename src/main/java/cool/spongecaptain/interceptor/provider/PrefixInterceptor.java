package cool.spongecaptain.interceptor.provider;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 要求消息的 key value 必须都为 String 类型的拦截器
 */
public class PrefixInterceptor implements ProducerInterceptor<String,String> {
    final String PREFIX = "Prefix From Spongecaptain: ";
    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord record) {
        //在这里为每一个消息的 Key 与 value 加上 prefix：`Prefix From Spongecaptain: `
        //但是由于 ProducerRecord 内封装的 key 与 value 都使用 final 进行封装，因此无法修改，只能重构一个 ProducerRecord 了；额

        return new ProducerRecord<>(
                record.topic(), record.partition(),
                record.timestamp(), PREFIX+record.key(),
                PREFIX+record.value(),record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("From Interceptor#onAcknowledgement: "+metadata);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
