package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Source3_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        //kafka地址
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //消费者group id
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //消费位置，latest为最新位置（可以设置为从头开始消费）
        properties.setProperty("auto.offset.reset", "latest");

        // 从文件读取数据
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(
                        //定义了从哪些主题中读取数据。可以是一个 topic，也可以是 topic列表，还可以是匹配所有想要读取的 topic 的正则表达式。
                        "sensor",
                        //DeserializationSchema 或者KeyedDeserializationSchema,SimpleStringSchema，是一个内置的 DeserializationSchema，它只是将字节数组简单地反序列化成字符串。
                        new SimpleStringSchema(),
                        //Properties 设置了 Kafka 客户端的一些属性
                        properties
                )
        );

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
