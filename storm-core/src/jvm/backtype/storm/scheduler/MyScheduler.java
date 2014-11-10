package storm;

import java.util.*;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
 * on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
 * the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
 *
 * In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
 * <pre>
 *     # give our supervisor a name: "special-supervisor"
 *     supervisor.scheduler.meta:
 *       name: "special-supervisor"
 * </pre>
 *
 * Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
 * <pre>
 *     # tell nimbus to use this custom scheduler
 *     storm.scheduler: "storm.DemoScheduler"
 * </pre>
 * @author xumingmingv May 19, 2012 11:10:43 AM
 */
public class MyScheduler implements IScheduler {

    private Random random = new Random();

    public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {

        System.out.println("************** WELCOME TO MyScheduler **************");

        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("special-topology");

        // make sure the special topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println("Our special topology DOES NOT NEED scheduling.");
            } else {
                System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                System.out.println("Needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("Needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));

                // List of supervisors
                ArrayList<SupervisorDetails> supervisors = new ArrayList<SupervisorDetails>(cluster.getSupervisors().values());
                System.out.println("Supervisors list:");
                for (SupervisorDetails s : supervisors)
                    System.out.println("Supervisor: " + s.getId() + " host:" + s.getHost());

                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName("special-topology").getId());
                if (currentAssignment != null)
                    System.out.println("Current assignments: " + currentAssignment.getExecutorToSlot());
                else
                    System.out.println("Current assignments: {}");

                // Loop through all executors that need to be assigned to a supervisor
                for (String k : componentToExecutors.keySet()) {

                    List<ExecutorDetails> executors = componentToExecutors.get(k);

                    // Choose a RANDOM supervisor
                    SupervisorDetails supervisor = supervisors.get(random.nextInt(supervisors.size()));
                    List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);

                    // if there is no available slots on this supervisor, free some.
                    if (availableSlots.isEmpty() && !executors.isEmpty()) {
                        System.out.println("Freeing all slots");
                        for (Integer port : cluster.getUsedPorts(supervisor)) {
                            cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
                        }
                    } else
                        System.out.println("No need to free slot");

                    // re-get the aviableSlots
                    availableSlots = cluster.getAvailableSlots(supervisor);

                    // Assign into one RANDOM slot.
                    int rand = random.nextInt(availableSlots.size());

                    cluster.assign(availableSlots.get(rand), topology.getId(), executors);
                    System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(rand).getNodeId() + ", " + availableSlots.get(rand).getPort() + "]");
                }

            } // end of else if (!needScheduling)
        } // end of if (topology != null)

        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        System.out.println("Let Storm's scheduler schedule the rest");
        new EvenScheduler().schedule(topologies, cluster);
    }

}
