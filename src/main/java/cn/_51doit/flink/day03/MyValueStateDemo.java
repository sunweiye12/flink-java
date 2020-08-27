package cn._51doit.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * 将KeyedStream调用map方法传入自定义的聚合Function，并且结合State进行计数
 * 1.可以正确的累加单词的次数吗？
 * 2.程序出现异常，能够容错吗？
 */
public class MyValueStateDemo {

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> countState;

            //在构造器方法之后，map方法之前执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态或恢复状态
                //使用状态的步骤：
                //1.定义一个状态描述器，状态的名称，存储数据的类型等
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(
                        "wc-state",
                        Integer.class
                );
                //2.使用状态描述从对应的StateBack器获取状态
                countState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                String currentKey = input.f0;
                Integer currentCount = input.f1;
                Integer historyCount = countState.value();
                if(historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + currentCount;
                //更新state
                countState.update(sum);
                return Tuple2.of(currentKey, sum);
            }
        });

        result.print();

        env.execute();


    }
}
