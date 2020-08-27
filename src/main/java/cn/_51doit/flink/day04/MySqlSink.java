package cn._51doit.flink.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySqlSink extends RichSinkFunction<Tuple2<String, Integer>> {

    private Connection connection = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //可以创建数据库连接
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123456");

    }


    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO t_wordcount VALUES (?, ?) ON DUPLICATE KEY UPDATE counts = ?");
        preparedStatement.setString(1, value.f0);
        preparedStatement.setLong(2, value.f1);
        preparedStatement.setLong(3, value.f1);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }


}
