package cn._51doit.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> num1 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        DataStreamSource<Integer> num2 = env.fromElements( 10, 11, 12);

        //unoin要求两个流的数据类型必须一致
        DataStream<Integer> union = num1.union(num2);

        union.print();

        env.execute();

    }
}
