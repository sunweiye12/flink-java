package cn._51doit.flink.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

//运行该程序要传入5个参数：ckdir gid topic redishost redisport
public class KafkaSourceToRedisDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果开启Checkpoint，偏移量会存储到哪呢？
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        //就是将job cancel后，依然保存对应的checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(args[0]));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 30000));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", args[1]);
        properties.setProperty("auto.offset.reset", "earliest");
        //properties.setProperty("enable.auto.commit", "false");
        //如果没有开启checkpoint功能，为了不重复读取数据，FlinkKafkaConsumer会将偏移量保存到了Kafka特殊的topic中（__consumer_offsets）
        //这种方式没法实现Exactly-Once
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(args[2], new SimpleStringSchema(), properties);

        //在Checkpoint的时候将Kafka的偏移量保存到Kafka特殊的Topic中，默认是true
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

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
        //调用RedisSink将计算好的结果保存到Redis中

        //创建Jedis连接的配置信息
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(args[3])
                .setPassword(args[4])
                .build();

        summed.addSink(new RedisSink<>(conf, new RedisWordCountMapper()));

        env.execute("KafkaSourceDemo");

    }


    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //指定写入Redis中的方法和最外面的大key的名称
            return new RedisCommandDescription(RedisCommand.HSET, "wc");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0; //将数据中的哪个字段作为key写入
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString(); //将数据中的哪个字段作为value写入
        }
    }
}


