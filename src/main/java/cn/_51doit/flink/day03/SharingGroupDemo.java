package cn._51doit.flink.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SharingGroupDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用Socket创建DataStream
        //socketTextStream是一个非并行的Source，不论并行度设置为多少，总是一个并行
        //DataStreamSourc并行度为1
        DataStreamSource<String> lines = env.socketTextStream("node-1.51doit.cn", 8888);

        //DataStream的并行度默认是使用你设置的并行度
        //DataStream并行度为4
        DataStream<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                Arrays.stream(line.split(" ")).forEach(w -> out.collect(Tuple2.of(w, 1)));
            }
        }).slotSharingGroup("doit");;

        //keyBy属于shuffle（redistribute）算子
        //KeyedStream并行度为4
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        //SingleOutputStreamOperator并行度为4
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyed.sum(1);

        result.print();

        env.execute();

    }
}
