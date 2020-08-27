package cn._51doit.flink.day02;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class StreamFileSinkDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> upper = lines.map(String::toUpperCase);

        String path = "file:///Users/xing/Desktop/out";

        env.enableCheckpointing(10000);

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(30)) //
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        upper.addSink(sink);

        env.execute();

    }
}
