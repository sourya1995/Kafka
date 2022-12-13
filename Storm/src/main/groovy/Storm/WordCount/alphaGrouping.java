package Storm.WordCount;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class alphaGrouping implements CustomStreamGrouping{
    private List<Integer> targetTasks;
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {

    }

    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<Integer>();
        String word = list.get(0).toString();
        if(word.startsWith("a")){
            boltIds.add(targetTasks.get(0));
        } else {
            boltIds.add(targetTasks.get(1));
        }
        return boltIds;
    }
}
