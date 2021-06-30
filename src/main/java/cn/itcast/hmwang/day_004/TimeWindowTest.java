package cn.itcast.hmwang.day_004;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @program: flink-study
 * @description: 测试flink 基于时间的窗口
 * @author: hemwang
 * @create: 2021-05-25 23:32
 **/
public class TimeWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> source = env.socketTextStream("pro1", 7777);
        SingleOutputStreamOperator<CarInfo> res = source.flatMap(new FlatMapFunction<String, CarInfo>() {
            @Override
            public void flatMap(String value, Collector<CarInfo> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new CarInfo(split[0], Integer.valueOf(split[1])));
            }
        }).keyBy(CarInfo::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("count");

        res.print();
        env.execute();
    }
    
    
}
