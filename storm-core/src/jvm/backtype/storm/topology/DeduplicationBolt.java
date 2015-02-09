package backtype.storm.topology;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashSet;

/**
 * Created by Enrico on 12/6/14.
 */
public abstract class DeduplicationBolt extends BaseBasicBolt {

	private HashSet<Object> set = new HashSet<Object>(); // The keys that we've seen so far // TODO: When do we clear this set?
	private String field; // The field used for dedup key

    /**
     * Public constructor
     *
     * @param field the field to be used as a key for de-duplication
     */
    public DeduplicationBolt(String field)
    {
        this.field = field;
    }

    /**
     * Storm will call this on every Tuple by default. However, since this method is declared final,
     * the user won't be able to override this method. Instead, the user must implement executeImpl.
     *
     * This method removes duplicates by using one of the values in the Tuple as a key.
     */
    @Override
    public final void execute(Tuple input, BasicOutputCollector collector)
    {
    	Object key = input.getValueByField(field);
        if (!set.contains(key)) {
            set.add(key);
            executeImpl(input, collector);
        }
    }

    /**
     * Implement this method instead of the original execute method. Rest assured that the input
     * tuples have been de-duplicated using the field that you declared when
     * constructing this class.
     */
    protected abstract void executeImpl(Tuple input, BasicOutputCollector collector);
}
