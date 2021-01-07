package cool.spongecaptain.serial.serializer;

import cool.spongecaptain.serial.dto.Cat;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CatSerializer implements Serializer<Cat> {

    @Override
    public byte[] serialize(String topic, Cat cat) {
        //简单一点设计：默认使用 UTF-8 编码
        //又因为 int 默认占 4 字节，因此无需记录 name 的长度，但是必须确保 int 对应的字节在 String 对应的字节之前
        String name = cat.getName();
        int age = cat.getAge();

        byte[] bytesOfAge = int2Bytes(age);
        byte[] bytesOfName = name.getBytes(StandardCharsets.UTF_8);

        return mergeBytes(bytesOfAge,bytesOfName);//注意合并的顺序不能颠倒
    }
    //int 转换为字节数组
    private static byte[] int2Bytes(int n){
        return new byte[] {
                (byte) ((n >> 24) & 0xFF),
                (byte) ((n >> 16) & 0xFF),
                (byte) ((n >> 8) & 0xFF),
                (byte) (n & 0xFF)
        };
    }
    //合并两个字节数组
    private static byte[] mergeBytes(byte[] b1, byte[] b2) {
        byte[] mergeBytes = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, mergeBytes, 0, b1.length);
        System.arraycopy(b2, 0, mergeBytes, b1.length, b2.length);
        return mergeBytes;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Headers headers, Cat cat) {
        return serialize(topic,cat);
    }

    @Override
    public void close() {

    }
}
