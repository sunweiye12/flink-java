package cn._51doit.flink.day03;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class MyAtLeastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private transient ListState<Long> listState;

    private boolean flag = true;
    private Long offset = 0L;

    //在构造方法之后，open方法之前执行一次，用于初始化Operator State或恢复Operator State
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //定义一个状态描述器
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>(
                "offset-state",
                Long.class
        );
        //listState中存储的就是一个long类型的数值
        listState = context.getOperatorStateStore().getListState(stateDescriptor);

        //从ListState中恢复数据
        if(context.isRestored()) {
            for (Long first : listState.get())
                offset = first;
        }
    }

    //snapshotState方法是在checkpoint时，会调用
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //将上一次checkpoint的数据情况
        listState.clear();
        //将最新的偏移量保存到ListState中
        listState.add(offset);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile raf = new RandomAccessFile("/Users/xing/Desktop/data/" + taskIndex + ".txt", "r");
        //从指定的位置读取数据
        raf.seek(offset);
        //获取一个checkpoint的锁
        final Object checkpointLock = ctx.getCheckpointLock();
        while (flag) {
            String line = raf.readLine();
            if(line != null) {
                //获取最新的偏移量
                synchronized (checkpointLock) {
                    line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                    offset = raf.getFilePointer();
                    ctx.collect(taskIndex + ".txt => "  + line);
                }
            } else {
                Thread.sleep(1000);
            }
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
