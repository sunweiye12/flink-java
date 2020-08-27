package cn._51doit.flink.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> words = env.fromElements("a", "b", "c", "d", "e");

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        ConnectedStreams<Integer, String> connected = nums.connect(words);

        SingleOutputStreamOperator<String> res = connected.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value * 10 + "";
            }

            @Override
            public String map2(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        res.print();

        env.execute();

    }
}
