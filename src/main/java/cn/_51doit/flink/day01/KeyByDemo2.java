package cn._51doit.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //辽宁省,沈阳市,1000

        SingleOutputStreamOperator<OrderBean> provinceCityAndMoney = lines.map(new MapFunction<String, OrderBean>() {

            @Override
            public OrderBean map(String line) throws Exception {
                String[] fields = line.split(",");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return new OrderBean(province, city, money);
            }
        });

        KeyedStream<OrderBean, Tuple> keyed = provinceCityAndMoney.keyBy("province", "city");

        SingleOutputStreamOperator<OrderBean> res = keyed.sum("money");

        //provinceCityAndMoney.keyBy(OrderBean::getProvince) 只能按照一个字段分组

        res.print();

        env.execute();

    }
}
