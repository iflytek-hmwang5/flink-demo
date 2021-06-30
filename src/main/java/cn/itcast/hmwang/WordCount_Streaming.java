package cn.itcast.hmwang;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 基于flink1.12.0 创建流批一体的WordCount
 */
public class WordCount_Streaming {
    public static void main(String[] args) throws Exception {
        // 0,配置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 1,构建数据源
        DataStreamSource<String> source = env.socketTextStream("pro1",7777);

        // 2,transfermation
        SingleOutputStreamOperator<String> words = source.flatMap((String value,Collector<String> out) -> Arrays.asList(value.split(" ")).forEach(x->out.collect(x))).returns(String.class);
//        SingleOutputStreamOperator<String> words = source.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                Arrays.asList(value.split(" ")).forEach(x -> out.collect(x));
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(x -> Tuple2.of(x, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return Tuple2.of(s,1);
//            }
//        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.keyBy(t->t.f0).sum(1);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return stringIntegerTuple2.f0;
//            }
//        }).sum(1);

        sum.print();

        // 3,sink
        env.execute();
    }
}
