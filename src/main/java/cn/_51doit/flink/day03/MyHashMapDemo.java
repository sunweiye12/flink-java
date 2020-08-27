package cn._51doit.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * 将KeyedStream调用map方法传入自定义的聚合Function
 * 1.可以正确的累加单词的次数吗？
 * 2.程序出现异常，能够容错吗？
 */
public class MyHashMapDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //开启checkpoint
        env.enableCheckpointing(5000); //开启checkpoint，默认的重启策略就是无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.equals("laoduan")) {
                    int i = 1 / 0; //模拟出现错误，任务重启
                }
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private HashMap<String, Integer> state = new HashMap<>();

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String currentKey = input.f0;
                Integer currentCount = input.f1;
                Integer historyCount = state.get(currentKey);
                if (historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + currentCount; //累加
                //更新状态数据（我自己实现的计数器）
                state.put(currentKey, sum);
                return Tuple2.of(currentKey, sum); //输出结果
            }
        });

        result.print();

        env.execute();


    }
}
