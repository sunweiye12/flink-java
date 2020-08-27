package cn._51doit.flink.day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class ParSourceDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        //通过env调用Source方法创建DataStream
        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1L, 10L), // 生成数组的range
                Long.class);

        int parallelism = nums.getParallelism();

        System.out.println("Source ====> " + parallelism);


        SingleOutputStreamOperator<Long> res = nums.filter(i -> i % 2 == 0);

        res.print();

        env.execute();


    }

}
