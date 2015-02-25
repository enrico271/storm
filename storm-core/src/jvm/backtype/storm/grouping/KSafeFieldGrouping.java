package backtype.storm.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class KSafeFieldGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks = null;
    private final int K;

    public KSafeFieldGrouping(int k) {
        this.K = k;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        if (values.size() > 0) {
            if (values.get(0) != null) {
                int hc = Math.abs(values.get(0).hashCode());
                int size = targetTasks.size();
                for (int i = 0; i < K + 1; i++) {
                    boltIds.add(targetTasks.get((hc + i) % size));
                }
            }
        }
        return boltIds;
    }

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }
}