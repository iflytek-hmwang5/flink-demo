package cn.itcast.hmwang.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study
 * @description: keyedstate使用案例
 * @author: hemwang
 * @create: 2021-06-02 22:33
 **/
public class KeyedState_Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Tuple2<String, Long>> source = env.fromElements(
                Tuple2.of("北京", 1l),
                Tuple2.of("上海", 3l),
                Tuple2.of("上海", 2l),
                Tuple2.of("上海", 5l),
                Tuple2.of("北京", 8l),
                Tuple2.of("北京", 3l)
        );
        SingleOutputStreamOperator<Tuple2<String, Long>> res1 = source
                .keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private ValueState<Long> maxValue;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> maxValueDesc = new ValueStateDescriptor<>("maxValue", Long.class);
                        maxValue = getRuntimeContext().getState(maxValueDesc);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> stringIntegerTuple2) throws Exception {
                        Long max = maxValue.value();
                        if(max == null || stringIntegerTuple2.f1 > max){
                            max = stringIntegerTuple2.f1;
                            maxValue.update(max);
                        }
                        return new Tuple2<>(stringIntegerTuple2.f0,max);
                    }
                });
//                .maxBy(1);


        res1.print();
        env.execute();
    }
}
