package backtype.storm.topology.ksafety;

/**
 * Created by Enrico on 3/27/15.
 *
 * This class is used to store additional data on every tuple for k-safety purposes
 */
public class KSafeData {

    public String sourceSpout;
    public long sequenceNumber;

    public KSafeData() {}

    public KSafeData(String sourceSpout, long sequenceNumber) {
        this.sourceSpout = sourceSpout;
        this.sequenceNumber = sequenceNumber;
    }
}
