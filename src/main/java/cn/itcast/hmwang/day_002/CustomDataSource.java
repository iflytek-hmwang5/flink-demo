package cn.itcast.hmwang.day_002;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @program: flink-study
 * @description: 自定义datasource
 * @author: hemwang
 * @create: 2021-05-17 22:05
 **/
public class CustomDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Order> orderDataStreamSource = env.addSource(new MyOrderDataSource());

        orderDataStreamSource.print();

        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    static class MyOrderDataSource extends RichParallelSourceFunction<Order> {

        private Boolean flag = true;

        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {
            Random random = new Random();
            while (flag){

                String id = UUID.randomUUID().toString();
                Integer userId = random.nextInt(3);
                Integer money = random.nextInt(101);
                Long createTime = System.currentTimeMillis();
                sourceContext.collect(new Order(id,userId,money,createTime));

                Thread.sleep(30000l);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

}