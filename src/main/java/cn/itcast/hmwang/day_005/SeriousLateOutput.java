package cn.itcast.hmwang.day_005;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @program: flink-study
 * @description: 使用侧道输出流保留超时严重的数据
 * @author: hemwang
 * @create: 2021-06-01 06:58
 **/
public class SeriousLateOutput {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Order> source = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    Integer userId = random.nextInt(2);
                    Integer money = random.nextInt(101);
                    Long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
        );

        OutputTag<Order> outputTag = new OutputTag<Order>("SeriousLateData");

        SingleOutputStreamOperator<Order> res1 = orderSingleOutputStreamOperator
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(outputTag)
                .sum("money");

        res1.print();
        DataStream<Order> res2 = res1.getSideOutput(outputTag);
        res2.print();

        env.execute();

    }
}
