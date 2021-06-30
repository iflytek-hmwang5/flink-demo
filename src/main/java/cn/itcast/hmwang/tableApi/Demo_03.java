package cn.itcast.hmwang.tableApi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @program: flink-study
 * @description: table api WordCount
 * @author: hemwang
 * @create: 2021-06-10 23:08
 **/
public class Demo_03 {


    public static void main(String[] args) throws Exception {
        // 1 ,构建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        // 2 ,add data source
        DataStreamSource<WC> source = env.fromElements(new WC("hello", 1L),
                new WC("world", 1L),
                new WC("hello", 1L));

        // 3 ,transformation
        Table table = tableEnv.fromDataStream(source);
        Table filter = table.groupBy($("word")).select($("word"), $("frequency").sum().as("frequency")).filter($("frequency").isEqual(2));

        // 4 ,sink
        DataStream<Tuple2<Boolean, WC>> tuple2DataStream = tableEnv.toRetractStream(filter, WC.class);
        tuple2DataStream.print();
        env.execute();


    }

}
