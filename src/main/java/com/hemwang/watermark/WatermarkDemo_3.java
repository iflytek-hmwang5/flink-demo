package com.hemwang.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @program: flink-study
 * @description: 侧道输出流获取延迟严重的数据
 * @author: hemwang
 * @create: 2021-07-11 10:02
 **/
public class WatermarkDemo_3 {

    public static void main(String[] args) throws Exception {
        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Order> sourceDs = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(100);
                    // 模拟数据延迟和乱序
                    long eventTime = System.currentTimeMillis() - random.nextInt(10) * 1000;
                    System.out.println("发送的数据为：" + userId + " : " + df.format(eventTime) + " : " + money);
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

//        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = sourceDs.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner(new TimestampAssignerSupplier<Order>() {
//                            @Override
//                            public TimestampAssigner<Order> createTimestampAssigner(Context context) {
//                                return (element, recordTimestamp) -> {
//                                    return element.getEventTime();
//                                };
//                            }
//                        }));
//
//        SingleOutputStreamOperator<Order> money = orderSingleOutputStreamOperator
//                .keyBy(Order::getUserId)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .sum("money");

//        money.print();

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = sourceDs.assignTimestampsAndWatermarks(new WatermarkStrategy<Order>() {
            @Override
            public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Order>() {

                    private int userId = 0;
                    private long eventTime = 0L;
                    private final long outOfOrdernessMillis = 3000;
                    private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

                    @Override
                    public void onEvent(Order event, long eventTimestamp, WatermarkOutput output) {
                        userId = event.getUserId();
                        eventTime = event.getEventTime();
                        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                        System.out.println("key:" + userId + ",系统时间:" + df.format(System.currentTimeMillis()) + ",事件时间:" + df.format(eventTime) + ",水印时间:" + df.format(watermark.getTimestamp()));
                        output.emitWatermark(watermark);
                    }
                };
            }
        }.withTimestampAssigner(((event, recordTimestamp) -> event.getEventTime())));

        final OutputTag<Order> lateDataTag = new OutputTag<Order>("lateData", TypeInformation.of(Order.class));

        SingleOutputStreamOperator<String> res = orderSingleOutputStreamOperator
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateDataTag)
                .apply(new WindowFunction<Order, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Order> input, Collector<String> out) throws Exception {
                        //用来存放当前窗口的数据的格式化后的事件时间
                        List<String> list = new ArrayList<>();
                        for (Order order : input) {
                            Long eventTime = order.eventTime;
                            String formatEventTime = df.format(eventTime);
                            list.add(formatEventTime);
                        }
                        String start = df.format(window.getStart());
                        String end = df.format(window.getEnd());
                        //现在就已经获取到了当前窗口的开始和结束时间,以及属于该窗口的所有数据的事件时间,把这些拼接并返回
                        String outStr = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s", key.toString(), start, end, list.toString());
                        out.collect(outStr);
                    }
                });
        DataStream<Order> sideOutput = res.getSideOutput(lateDataTag);

        sideOutput.print("延迟数据");

        res.print("正常数据");



        env.execute();


    }





    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
