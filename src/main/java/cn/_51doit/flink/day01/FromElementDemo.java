package cn._51doit.flink.day01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class FromElementDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //通过env调用Source方法创建DataStream
        DataStreamSource<Integer> nums = env.fromElements(1,2,3,4,5,6,7,8,9);

        SingleOutputStreamOperator<Integer> even = nums.filter(i -> i % 2 == 0);

        even.print();

        env.execute();

    }
}
