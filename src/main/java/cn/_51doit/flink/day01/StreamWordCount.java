package cn._51doit.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        //创建一个Stream计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //调用Source创建DataStream 开始
        DataStreamSource<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        int parallelism = lines.getParallelism();

        System.out.println("Source ====> " + parallelism);

        //Transformation 开始
//        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String line, Collector<String> out) throws Exception {
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    out.collect(word);
//                }
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String word) throws Exception {
//                //return new Tuple2<>(word, 1);
//                return Tuple2.of(word, 1);
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        //Transformation 结束

        //调用Sink
        summed.print();

        //执行程序
        env.execute("StreamWordCount");
    }
}
