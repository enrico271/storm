package backtype.storm.topology.ksafety;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Use this bolt whenever you are using k-safety
 *
 * Created by Enrico on 3/27/15.
 */
public abstract class KSafeBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, HashMap<Integer, Long>> dedupMap = new HashMap<>();
    private HashMap<String, HashMap<Integer, Object>> states = new HashMap<>();
    private int numTasks = 0;

    /**
     * Storm calls this method after this component is deployed on the cluster. User needs to implement prepareImpl instead
     * of this method.
     *
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        prepareImpl(stormConf, context);
    }

    /**
     * Implement this method instead of implementing open.
     *
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     */
    protected abstract void prepareImpl(Map stormConf, TopologyContext context);

    /**
     * Storm will call this on every tuple by default. However, since this method is declared final,
     * the user won't be able to override this method. Instead, the user must implement executeImpl.
     *
     * @param input the input tuple to be processed
     */
    @Override
    public final void execute(Tuple input) {
        KSafeInfo info = (KSafeInfo) input.getValueByField(KSafeUtils.EXTRA_FIELD);

        if (!info.fromSpout) {

            if (!dedupMap.containsKey(info.spoutId))
                dedupMap.put(info.spoutId, new HashMap<Integer, Long>());

            HashMap<Integer, Long> taskToSeqNum = dedupMap.get(info.spoutId);

            if (taskToSeqNum.containsKey(info.primaryTask)) {
                long maxSeqNum = taskToSeqNum.get(info.primaryTask);
                long thisSeqNum = info.sequenceNumber;

                if (thisSeqNum <= maxSeqNum && maxSeqNum - thisSeqNum < Long.MAX_VALUE / 2) {
                    // Duplicate tuple, ignore
                    // System.out.println("Duplicate: thisSeqnum: " + thisSeqNum + " maxSeqnum: " + maxSeqNum);
                    return;
                }
                else
                    // Not duplicate, update max sequence number
                    taskToSeqNum.put(info.primaryTask, info.sequenceNumber);
            }
            else
                // Not duplicate, update max sequence number
                taskToSeqNum.put(info.primaryTask, info.sequenceNumber);
        }

        executeImpl(input);
    }

    /**
     * Implement this method instead of the original execute method. Rest assured that the input
     * tuples have been de-duplicated.
     *
     * @param input the input tuple to be processed
     */
    protected abstract void executeImpl(Tuple input);

    /**
     * Use this method to emit a tuple instead of calling emit on an OutputCollector.
     *
     * @param input the input tuple that causes this output tuple
     * @param output the output tuple to emit as a list of values
     */
    protected final void emit(Tuple input, List<Object> output) {
        KSafeInfo info = (KSafeInfo) input.getValueByField(KSafeUtils.EXTRA_FIELD);
        info.primaryTask = KSafeUtils.chooseTask(input.getValues(), numTasks);
        info.fromSpout = false;
        output.add(info);
        this.collector.emit(output);
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
     * @return must be implemented to return the output fields
     */
    protected abstract Fields declareOutputFieldsImpl();

    /**
     * Set a state
     *
     * @param input the input tuple that causes this operation
     * @param name the name of the state
     * @param value the value of the state to set to
     */
    protected final void setState(Tuple input, String name, Object value) {
        if (!states.containsKey(name))
            states.put(name, new HashMap<Integer, Object>());
        int primaryTask = KSafeUtils.chooseTask(input.getValues(), numTasks);
        states.get(name).put(primaryTask, value);
    }

    /**
     * Get a state
     *
     * @param input the input tuple that causes this operation
     * @param name the name of the state
     * @return the value of the requested state
     */
    protected final Object getState(Tuple input, String name) {
        if (!states.containsKey(name))
            states.put(name, new HashMap<Integer, Object>());
        int primaryTask = KSafeUtils.chooseTask(input.getValues(), numTasks);
        return states.get(name).get(primaryTask);
    }
}