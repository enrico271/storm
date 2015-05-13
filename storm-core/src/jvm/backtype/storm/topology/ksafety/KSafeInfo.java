package backtype.storm.topology.ksafety;

/**
 * Created by Enrico on 3/27/15.
 *
 * This class is used to store additional data on every tuple for k-safety purposes
 */
public class KSafeInfo {

    public String spoutId;
    public boolean fromSpout;
    public long sequenceNumber;
    public int primaryTask;

    @SuppressWarnings("unused")
    private KSafeInfo() {} // Kryo needs this

    public KSafeInfo(String spoutId, long sequenceNumber) {
        this.spoutId = spoutId;
        this.sequenceNumber = sequenceNumber;
        this.fromSpout = true;
    }
}
