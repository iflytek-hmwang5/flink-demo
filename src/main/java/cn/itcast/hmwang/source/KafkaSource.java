package cn.itcast.hmwang.source;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @program: flink-study
 * @description: Kafka数据源
 * @author: hemwang
 * @create: 2021-06-27 08:37
 **/
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // must
        env.enableCheckpointing(1000);
        if(SystemUtils.IS_OS_WINDOWS){
            env.setStateBackend(new FsStateBackend("file:///D:/checkpoint"));
        }else {
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"));
        }
        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "localhost:9092");
        properties1.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties1);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        DataStream<String> kafkaSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> result = kafkaSource.flatMap (new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(t -> t.f0)
                .sum(1)
                .map(t -> t.f0 + ":" + t.f1).returns(String.class);


        Properties properties2 = new Properties();
        properties2.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>(
                "my-topic",                  // target topic
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),   // serialization schema
                properties2,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        result.addSink(kafkaSink);

        env.execute();
    }
}
