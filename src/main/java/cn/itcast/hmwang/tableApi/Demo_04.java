package cn.itcast.hmwang.tableApi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @program: flink-study
 * @description: 每隔5s统计每个用户的订单总金额，最大金额，最小金额
 * @author: hemwang
 * @create: 2021-06-14 09:03
 **/
public class Demo_04 {
    public static void main(String[] args) throws Exception {
        // 1 ,build execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2 ,add data source
        DataStreamSource<Order> orderDS  = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        // 3 ,set event time and watermark
        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
        );


        // 4 ,transformation
        SingleOutputStreamOperator<Order> money = orderSingleOutputStreamOperator.keyBy(Order::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .sum("money");

        money.print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;//事件时间
    }
}
