package cn._51doit.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MinMaxDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //省份,城市,人数
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> provinceCityAmount = lines.map(line -> {
            String[] fields = line.split(",");
            String province = fields[0];
            String city = fields[1];
            Integer amount = Integer.parseInt(fields[2]);
            return Tuple3.of(province, city, amount);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyed = provinceCityAmount.keyBy(0);

        //min、max返回分组的字段和参与比较的数据，如果有多个字段，其他字段的返回值是第一次出现的数据。
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> max = keyed.max(2);

        max.print();

        env.execute();
    }

}
