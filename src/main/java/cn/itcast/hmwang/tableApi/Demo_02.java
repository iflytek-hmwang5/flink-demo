package cn.itcast.hmwang.tableApi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @program: flink-study
 * @description: table sql api WordCount
 * @author: hemwang
 * @create: 2021-06-10 23:08
 **/
public class Demo_02 {

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
        tableEnv.createTemporaryView("t_wc",source,$("word"),$("frequency"));

        String sql = "select word,sum(frequency) as frequency from t_wc group by word";

        Table table = tableEnv.sqlQuery(sql);

        // 4 ,sink
        DataStream<Tuple2<Boolean, WC>> tuple2DataStream = tableEnv.toRetractStream(table, WC.class);
        tuple2DataStream.print();

        env.execute();
    }


}
