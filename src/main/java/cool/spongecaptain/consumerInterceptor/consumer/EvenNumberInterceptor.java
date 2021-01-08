package cool.spongecaptain.consumerInterceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 这是一个要求消息的 key 以及 value 都是 String 类型的 Consumer 端拦截器
 */
public class EvenNumberInterceptor implements ConsumerInterceptor<String,String> {
    //ConsumerRecords 被拉取到 Consumer 端，在真正可以被 poll 返回之前，会调用此方法
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {

        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords
                = new HashMap<>();
        //遍历记录中的所有主题+分区
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            //我们仅仅通过 key 的最后一个数为偶数的 record
            for (ConsumerRecord<String, String> record : tpRecords) {
                if(((Integer.parseInt(record.key().substring(record.key().length()-1))&1)==0)){
                    //偶数，则加入我们最后返回的集合中
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    //在提交消费位移之前，会调用此次方法，领先于 OffsetCommitCallback 的回调逻辑
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("interceptor 回调");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
