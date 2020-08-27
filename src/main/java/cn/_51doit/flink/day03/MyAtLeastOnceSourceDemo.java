package cn._51doit.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceSourceDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //自定义一个多并行的Source
        DataStreamSource<String> lines1 = env.addSource(new MyAtLeastOnceSource());

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> error = lines2.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                if (line.startsWith("error")) {
                    int i = 1 / 0;
                }
                return line;
            }
        });

        DataStream<String> union = lines1.union(error);

        union.print();

        env.execute();

    }
}
