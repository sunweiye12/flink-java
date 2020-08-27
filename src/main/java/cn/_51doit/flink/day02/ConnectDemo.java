package cn._51doit.flink.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env
        DataStreamSource<String> line1 = env.socketTextStream("localhost", 8888);

        DataStreamSource<String> line2 = env.socketTextStream("localhost", 9999);

        ConnectedStreams<String, String> connected = line1.connect(line2);

        SingleOutputStreamOperator<String> res = connected.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(String value) throws Exception {
                return value + " doit";
            }
        });

        res.print();

        env.execute();

    }
}
