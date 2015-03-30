package backtype.storm.topology.ksafety;

import java.util.List;

/**
 * Created by Enrico on 3/27/15.
 */
public final class KSafeUtils {

    private KSafeUtils() {}

    public static String EXTRA_FIELD = "__pikachu";

    public static int chooseTask(List<Object> values, int numTasks) {
        return Math.abs(values.get(0).hashCode()) % numTasks;
    }
}
