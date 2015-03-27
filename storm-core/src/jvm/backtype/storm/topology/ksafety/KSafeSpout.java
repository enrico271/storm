package backtype.storm.topology.ksafety;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

/**
 * Use this spout whenever you are using k-safety
 *
 * Created by Enrico on 3/27/15.
 */
public abstract class KSafeSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String id;
    private long counter = 0;

    /**
     * Storm calls this method after this component is deployed on the cluster. User needs to implement openImpl instead
     * of this method.
     *
     * @param conf The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
     */
    @Override
    public final void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.id = context.getThisComponentId() + "-" + context.getThisTaskIndex();
        openImpl(conf, context);
    }

    /**
     * Implement this method instead of implementing open.
     *
     * @param conf The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     */
    protected abstract void openImpl(Map conf, TopologyContext context);

    /**
     * Use this method to emit a tuple instead of calling emit on an OutputCollector.
     * Usage is the same as how you call emit on an OutputCollector.
     *
     * @param tuple the output tuple to emit
     */
    protected void emit(List<Object> tuple) {
        tuple.add(new KSafeData(id, counter++));
        this.collector.emit(tuple);
    }

    /**
     * Storm calls this method to declare the output field names. User must implement declareOutputFieldsImpl
     * instead of calling this method.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = declareOutputFieldsImpl();
        List<String> list = fields.toList();
        list.add(KSafeUtils.EXTRA_FIELD); // adds an extra field

        declarer.declare(new Fields(list));
    }

    /**
     * Implement this method to declare output fields instead of implementing the regular declareOutputFields
     *
     * @return must be implemented to return the field names to declare
     */
    protected abstract Fields declareOutputFieldsImpl();
}
