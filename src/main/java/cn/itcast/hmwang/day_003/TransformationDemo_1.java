package cn.itcast.hmwang.day_003;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @program: flink-study
 * @description: flink transformation operator 拆分流
 * @author: hemwang
 * @create: 2021-05-18 22:43
 **/
public class TransformationDemo_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Long> source = env.fromElements(1l,2l,3l,4l,5l,6l,7l,8l);
        OutputTag<Long> odd = new OutputTag<Long>("odd", TypeInformation.of(Long.class));
        OutputTag<Long> even = new OutputTag<Long>("even",TypeInformation.of(Long.class));

        SingleOutputStreamOperator<Long> process = source.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, Context context, Collector<Long> collector) throws Exception {
                if (value % 2 == 0) {
                    context.output(even, value);
                } else {
                    context.output(odd, value);
                }
            }
        });
        DataStream<Long> rs1 = process.getSideOutput(even);
        DataStream<Long> rs2 = process.getSideOutput(odd);
        rs1.print("偶数");
        rs2.print("奇数");
        env.execute();
    }


}
