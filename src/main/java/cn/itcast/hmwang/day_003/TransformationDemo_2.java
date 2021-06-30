package cn.itcast.hmwang.day_003;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @program: flink-study
 * @description: flink transformation operator rebalance
 * @author: hemwang
 * @create: 2021-05-18 22:43
 **/
public class TransformationDemo_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Long> source = env.fromSequence(0,100);
        SingleOutputStreamOperator<Long> filter = source.filter(x -> x >= 10);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> sum = filter.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int tid = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(tid, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> sum1 = filter.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int tid = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(tid, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        sum.print("sum");
        sum1.print("sum1");
        env.execute();
    }


}
