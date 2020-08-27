package cn._51doit.flink.day02;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;

public class SplitDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        SplitStream<Integer> splited = nums.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                //定义一个集合用于装标签的名称
                ArrayList<String> tags = new ArrayList<>();
                if (value % 2 == 0) {
                    //打标签,偶数,将当前的数据和标签名称进行一一的关联
                    tags.add("even");
                } else {
                    //打标签，奇数
                    tags.add("odd");
                }
                return tags;
            }
        });

        DataStream<Integer> even = splited.select("even");
        DataStream<Integer> odd = splited.select("odd");

        //even.print("this is even => ");
        //odd.print("this is odd => ");

        //有多种类型，可以根据标签名称进行select
        DataStream<Integer> selected = splited.select("even", "odd");

        selected.print();

        env.execute();

    }
}
