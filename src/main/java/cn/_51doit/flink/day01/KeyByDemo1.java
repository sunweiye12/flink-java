package cn._51doit.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //辽宁省,沈阳市,1000

        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {

            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return Tuple3.of(province, city, money);
            }
        });

        KeyedStream<Tuple3<String, String, Double>, Tuple> keyed = provinceCityAndMoney.keyBy(0, 1);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> summed = keyed.sum(2);

        summed.print();


        env.execute();

    }
}
