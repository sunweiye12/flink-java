package cn._51doit.flink.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class RestartStrategy {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //开启checkpoint
        //env.enableCheckpointing(5000); //开启checkpoint，默认的重启策略就是无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //env.setRestartStrategy(RestartStrategies.noRestart());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.equals("laoduan")) {
                    int i = 1 / 0;
                }
                return Tuple2.of(word, 1);
            }
        });

        //keyBy属于shuffle（redistribute）算子
        //KeyedStream并行度为4
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        //SingleOutputStreamOperator并行度为4
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum(1);

        result.print();

        env.execute();


    }
}
