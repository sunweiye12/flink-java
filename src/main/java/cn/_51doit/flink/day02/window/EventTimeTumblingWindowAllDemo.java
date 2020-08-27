package cn._51doit.flink.day02.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventTimeTumblingWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink默认使用ProcessingTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置EventTime作为时间标准

        //需要将时间转成Timestamp格式
        //2020-03-01 00:00:00,1
        //2020-03-01 00:00:04,2
        //2020-03-01 00:00:05,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中的EventTime
        SingleOutputStreamOperator<String> dataStreamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                String dateStr = fields[0];
                try {
                    Date date = sdf.parse(dateStr);
                    long timestamp = date.getTime();
                    return timestamp;
                } catch (ParseException e) {
                    throw new RuntimeException("时间转换异常");
                }
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
