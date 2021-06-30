package cn.itcast.hmwang.day_003;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @program: flink-study
 * @description: flink transformation operator (connect and union)
 * @author: hemwang
 * @create: 2021-05-18 22:43
 **/
public class TransformationDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH)
        DataStreamSource<String> source = env.fromElements("flink1", "hadoop1", "spark1");
        DataStreamSource<String> source1 = env.fromElements("flink2", "hadoop2", "spark2");
        DataStreamSource<Integer> source2 = env.fromElements(1, 2, 3, 4, 5);

        DataStream<String> union = source.union(source1); // union 能连接多个数据类型相同的数据源
//        source.union(source2) // union 不能连接数据类型不同的数据源
        ConnectedStreams<String, String> connect = source.connect(source1);// connect 能链接两个数据类型相同的数据源
        ConnectedStreams<String, Integer> connect1 = source.connect(source2);//connect 能链接两个数据类型不同的数据源

//        union.print();
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "String1:" + s;
            }

            @Override
            public String map2(String s) throws Exception {
                return "String2:" + s;
            }
        });
        map.print();
        env.execute();
    }


}
