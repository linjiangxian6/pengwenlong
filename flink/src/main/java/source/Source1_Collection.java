package source;

import beans.Temperature;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Source1_Collection {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置该程序并行度为1
        env.setParallelism(1);

        // 从集合中读取数据
        DataStream<Temperature> dataStream = env.fromCollection(Arrays.asList(
                new Temperature("sensor_1", 1547718199L, 35.8),
                new Temperature("sensor_6", 1547718201L, 15.4),
                new Temperature("sensor_7", 1547718202L, 6.7),
                new Temperature("sensor_10", 1547718205L, 38.1)
        ));

        dataStream.print();

        env.execute();


    }

}
