package cn.itcast.hmwang.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @program: flink-study
 * @description: 自定义source类
 * @author: hemwang
 * @create: 2021-06-03 06:57
 **/
public class MySource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private ListState<Long> offsetState = null;
    private boolean flag = true;
    private Long offset = 0l;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        offsetState.clear();
//        offsetState.add(offset);
        offsetState.update(Arrays.asList(offset));
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
