package sink;

import beans.Temperature;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class Sink4_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        //kafka位置
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //消费者group id
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //写入kafka位置
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new Temperature(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>(
                        //设置 kafka地址
                        "localhost:9092",
                        //设置 写入的topic
                        "sinktest",
                        //设置序列化类型
                        new SimpleStringSchema()
                )
        );

        env.execute();

    }
}
