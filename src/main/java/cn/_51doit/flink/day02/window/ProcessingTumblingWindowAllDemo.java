package cn._51doit.flink.day02.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class ProcessingTumblingWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //如果是划分窗口，如果没有调用keyBy分组（Non-Keyed Stream），调用windowAll
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //划分滚动窗口
        AllWindowedStream<Integer, TimeWindow> window = nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> sum = window.sum(0);

        sum.print();


        env.execute();
    }
}
