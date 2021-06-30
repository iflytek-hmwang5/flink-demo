package cn.itcast.hmwang.tableApi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @program: flink-study
 * @description: 案例一
 * @author: hemwang
 * @create: 2021-06-10 22:29
 **/
public class Demo_01 {
    public static void main(String[] args) throws Exception {
        // 1 ,构建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2 ,构建数据源
        DataStreamSource<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        // 3 ,Transformation
        Table tableA = tableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tableA.printSchema();

        tableEnv.createTemporaryView("tableB",orderB,$("user"),$("product"),$("amount"));

        /**
         * select * from tableA where user > 2
         * union
         * select * from tableB where user > 2
         */


        String sql = "select * from "+tableA+" where user > 2 " +
                "union " +
                "select * from tableB where user > 2";

        Table table = tableEnv.sqlQuery(sql);

        Table user = table.select($("user"),$("product"),$("amount"));
        table.printSchema();

        DataStream<Tuple2<Boolean, Order>> tuple2DataStream = tableEnv.toRetractStream(user, Order.class);

        tuple2DataStream.print();

        env.execute();


    }
}
