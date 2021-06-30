package cn.itcast.hmwang.state;

/**
 * @program: flink-study
 * @description: test
 * @author: hemwang
 * @create: 2021-06-03 21:45
 **/
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * Author itcast
 * Desc 使用OperatorState中的ListState模拟KafkaSource进行offset维护
 */
public class StateDemo02_OperatorState {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);//并行度设置为1方便观察
        //下面的Checkpoint和重启策略配置先直接使用,下次课学
        env.enableCheckpointing(1000);//每隔1s执行一次Checkpoint
        env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        //TODO 1.source
        DataStreamSource<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);

        //TODO 2.transformation

        //TODO 3.sink
        ds.print();

        //TODO 4.execute
        env.execute();
    }
    //使用OperatorState中的ListState模拟KafkaSource进行offset维护
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        private ListState<Long> offsetState = null;
        private boolean flag = true;
        private Long offset = 0l;

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            offsetState.clear();
            offsetState.add(offset);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<Long> offsetDesc = new ListStateDescriptor<>("offset", Long.class);
            offsetState = functionInitializationContext.getOperatorStateStore().getListState(offsetDesc);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (flag){
                Iterator<Long> iterator = offsetState.get().iterator();
                if(iterator.hasNext()){
                    offset = iterator.next();
                }
                offset+=1;
                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                ctx.collect("subTaskId:"+ subTaskId + ",当前的offset值为:"+offset);
                Thread.sleep(1000);

                //模拟异常
                if(offset % 5 == 0){
                    throw new Exception("bug出现了.....");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
