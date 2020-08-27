package cn._51doit.flink.day02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPrintSink<T> extends RichSinkFunction<T> {

    @Override
    public void invoke(T value, Context context) throws Exception {

        int index = getRuntimeContext().getIndexOfThisSubtask();

        System.out.println(index + " > " + value);
    }
}
