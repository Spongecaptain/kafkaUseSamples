package cool.spongecaptain.serial.deserializer;

import cool.spongecaptain.serial.dto.Cat;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Map;

public class CatDeserializer implements Deserializer<Cat> {

    @Override
    public Cat deserialize(String topic, byte[] data) {
        //首先是对 byte 前四个字节进行解码
        byte[] bytesOfInt = Arrays.copyOf(data,4);
        int age = byteArray2Int(bytesOfInt);
        //其余字节数据都属于 String 类型数据，注意事项：下面方法中的 4 对应字节数组中第 5 个元素，因为 0,1,2,3,4(第 5 个元素)
        byte[] bytesOfString = Arrays.copyOfRange(data,4,data.length);//左闭右开
        String name = new String(bytesOfString);

        return new Cat(name,age);
    }

    //字节数组转换为 int 值返回
    private static int byteArray2Int(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Cat deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic,data);
    }

    @Override
    public void close() {

    }
}
