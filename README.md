# README

## 1. Mac 下的 Kafka 安装与启动

**1.安装**

```bash
brew install kafka
brew install zookeeper
```

**2.修改配置**

修改 /usr/local/etc/kafka/server.properties, 找到 `listeners=PLAINTEXT://:9092` 那一行，把注释取消掉。然后修改为：

```
listeners=PLAINTEXT://localhost:9092
```

**3.启动**

> Kafka 的 sh 目录在： /usr/local/Cellar/kafka 下的版本号目录中下的 bin 目录下。

如果想以服务的方式启动，那么可以：

```bash
$ brew services start zookeeper
$ brew services start kafka
```

如果只是临时启动，可以：

```bash
$ zkServer start
$ kafka-server-start /usr/local/etc/kafka/server.properties
```

**4.关闭**

```bash
$ brew services stop zookeepera
$ brew services stop kafka
```

**5.查看运行状态**

```bash
$ brew services
```

上述命令能够查看所有由 brew 启动的服务的当前状态。

## 2. Kafka 数据的清空

为了避免运行时由于累计消息问题出现结果不一致，可以事先将 Kafka topic 下的消息进行手动清空。

执行如下命令找到日志存在的文件：

```bash
 cat /usr/local/etc/kafka/server.properties | grep "log.dirs"
```

然后递归删除上述文件夹下所有文件即可。

---

## 3. Provider 与 Consumer 的简单使用案例

> 见 simpleSample 模块。

按照先后顺序执行如下两个类的 Main 方法：

1. KafkaConsumerSample
2. KafkaProducerSample

最终，我们可以在控制台上看到如下输出：

```
offset = 0, key = SpongecaptainKey0, value = SpongecaptainValue0 
offset = 1, key = SpongecaptainKey1, value = SpongecaptainValue1 
offset = 2, key = SpongecaptainKey2, value = SpongecaptainValue2 
offset = 3, key = SpongecaptainKey3, value = SpongecaptainValue3 
offset = 4, key = SpongecaptainKey4, value = SpongecaptainValue4 
offset = 5, key = SpongecaptainKey5, value = SpongecaptainValue5 
offset = 6, key = SpongecaptainKey6, value = SpongecaptainValue6 
offset = 7, key = SpongecaptainKey7, value = SpongecaptainValue7 
offset = 8, key = SpongecaptainKey8, value = SpongecaptainValue8 
offset = 9, key = SpongecaptainKey9, value = SpongecaptainValue9 
```

其余模块的运行机制都类似，下面就不再赘述。

## 4. Callback回调机制

> 见 callback 模块。


## 5. 拦截器机制

> 见 interceptor 模块。

## 6. 序列化与反序列化

> 见 serial 模块。