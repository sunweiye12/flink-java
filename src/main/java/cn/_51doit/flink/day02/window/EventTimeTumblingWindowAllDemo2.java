package cn._51doit.flink.day02.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventTimeTumblingWindowAllDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink默认使用ProcessingTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置EventTime作为时间标准

        //需要将时间转成Timestamp格式
        //1582992000001,1
        //1582992003000,2
        //1582992004000,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中的EventTime字段，并且转换成Timestamp格式
        SingleOutputStreamOperator<String> dataStreamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Integer> nums = dataStreamWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                String[] fields = value.split(",");
                String numStr = fields[1];
                return Integer.parseInt(numStr);
            }
        });

        //如果是划分窗口，如果没有调用keyBy分组（Non-Keyed Stream），调用windowAll
        AllWindowedStream<Integer, TimeWindow> window = nums
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> sum = window.sum(0);

        sum.print();

        env.execute();
    }
}
