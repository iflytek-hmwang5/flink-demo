package com.hemwang.day001;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlinkBatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "D:\\work\\soft\\IntelliJ IDEA Community Edition 2020.2\\worksp\\flink-study\\src\\main\\resources\\words.txt";

        DataSource<String> inputDataSet = env.readTextFile(inputPath);

        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        stringTuple2FlatMapOperator.groupBy(0).sum(1).print();


    }
}
