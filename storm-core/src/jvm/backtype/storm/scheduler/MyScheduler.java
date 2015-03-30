package storm;

import java.util.*;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class MyScheduler implements IScheduler {

    private static org.apache.log4j.Logger LOG;

    private Random random = new Random();

    public void prepare(Map conf) {}

    public MyScheduler()
    {
        super();
        LOG = org.apache.log4j.Logger.getLogger(MyScheduler.class);
        LOG.info("Hello MyScheduler!");
    }


    public void schedule(Topologies topologies, Cluster cluster) {

        LOG.info("**************WELCOME TO MyScheduler **************");

        for (TopologyDetails t : topologies.getTopologies())
            scheduleATopology(t, cluster);
    }

    /*
     * Handle scheduling a single topology
     */
    private void scheduleATopology(TopologyDetails topology, Cluster cluster)
    {
        if (topology == null)
            return;

        LOG.info("PRINTING CURRENT ASSIGNMENTS");
        SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
        if (currentAssignment != null)
            LOG.info("Current assignments: " + currentAssignment.getExecutorToSlot());
        else
            LOG.info("Current assignments: {}");

        if (!cluster.needsScheduling(topology)) {
            System.out.println("Topology " + topology.getName() + " does not need scheduling");
            return;
        }

        // find out all the needs-scheduling components of this topology
        Map<String, List<ExecutorDetails>> needsSchedulingComponentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

        LOG.info("Needs scheduling(component->executor): " + needsSchedulingComponentToExecutors);
        System.out.println("Needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));

        // List of supervisors
        ArrayList<SupervisorDetails> supervisors = new ArrayList<SupervisorDetails>(cluster.getSupervisors().values());

        System.out.println("Supervisors list:");
        for (SupervisorDetails s : supervisors)
            System.out.println("Supervisor: " + s.getId() + " host:" + s.getHost());

        //
        Map<String, List<SupervisorDetails>> componentToAssignedSupervisors = getComponentToAssignedSupervisors(cluster, currentAssignment, topology);

        if (currentAssignment != null)
            System.out.println("Current assignments: " + currentAssignment.getExecutorToSlot());
        else
            System.out.println("Current assignments: {}");



        // Loop through all executors that need to be assigned to a supervisor
        for (String k : needsSchedulingComponentToExecutors.keySet()) {
            System.out.println("Starting Assignments for component: " + k);
            ArrayList<SupervisorDetails> unusedSupervisors = new ArrayList<SupervisorDetails>(cluster.getSupervisors().values());
            unusedSupervisors.removeAll(componentToAssignedSupervisors.get(k));

            List<ExecutorDetails> executors = needsSchedulingComponentToExecutors.get(k);

            //Assign each executor to a different unassigned Supervisor
            for(ExecutorDetails executor: executors) {
                List<ExecutorDetails> executorToAssign = new ArrayList<ExecutorDetails>();
                executorToAssign.add(executor);
				SupervisorDetails bestSupervisor = null;
				int mostSlots = -1;
                if (unusedSupervisors.size() !=  0) {
					for(SupervisorDetails supervisor: unusedSupervisors){
						List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
						if(availableSlots.size() > mostSlots){
							bestSupervisor = supervisor;
							mostSlots = availableSlots.size();
						}
					}
					List<WorkerSlot> availableSlots = cluster.getAvailableSlots(bestSupervisor);
                    System.out.println("PRINTING AVAILABLE SLOTS");
                    for (WorkerSlot slot : availableSlots) {
                        System.out.println(slot.toString());
                    }

                    // if there is no available slots on this supervisor, free some.
                    if (availableSlots.isEmpty() && !executors.isEmpty()) {
                        LOG.info("Freeing all slots for a supervisor");
                        for (Integer port : cluster.getUsedPorts(bestSupervisor)) {
                            cluster.freeSlot(new WorkerSlot(bestSupervisor.getId(), port));
                        }
                    } else
                        System.out.println("No need to free slot");

                    // re-get the aviableSlots
                    availableSlots = cluster.getAvailableSlots(bestSupervisor);

                    // Assign into one RANDOM slot.
                    int rand = random.nextInt(availableSlots.size());

                    cluster.assign(availableSlots.get(rand), topology.getId(), executorToAssign);
                    unusedSupervisors.remove(bestSupervisor);
                    LOG.info("We assigned executor:" + executor + " to slot: [" + availableSlots.get(rand).getNodeId() + ", " + availableSlots.get(rand).getPort() + "]");
                }
                else{
                    LOG.info("There are no remaining supervisors to assign this task: " + executor);
                }
            }
        }

    }

    /*
     * Return a map from components to assigned supervisors
     */
    private Map<String, List<SupervisorDetails>> getComponentToAssignedSupervisors(Cluster cluster, SchedulerAssignment currentAssignment, TopologyDetails topology){

        Map<String, List<SupervisorDetails>> componentToSupervisors = new HashMap<String, List<SupervisorDetails>>();
        Map<String, List<ExecutorDetails>> componentToExecutors = getComponentToExecutors(topology);
        //initialize map
        for(String component: componentToExecutors.keySet()){
            componentToSupervisors.put(component, new ArrayList<SupervisorDetails>());
        }

        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();


        if (currentAssignment != null) {
            Map<ExecutorDetails, WorkerSlot> executorToSlot = currentAssignment.getExecutorToSlot();
            Map<ExecutorDetails, String> executorToComponent = topology.getExecutorToComponent();

            //for each assigned executor, add the supervisor that its assigned to to the componentToSupervisors map
            for (ExecutorDetails executor : executorToSlot.keySet()) {
                String component = executorToComponent.get(executor);
                String supervisorId = executorToSlot.get(executor).getNodeId();
                componentToSupervisors.get(component).add(supervisors.get(supervisorId));
            }
        }

        return componentToSupervisors;
    }

    public Map<String, List<ExecutorDetails>> getComponentToExecutors(TopologyDetails topology) {
        Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<String, List<ExecutorDetails>>();
        for (ExecutorDetails executor : topology.getExecutorToComponent().keySet()) {
            String component = topology.getExecutorToComponent().get(executor);
            if (!componentToExecutors.containsKey(component)) {
                componentToExecutors.put(component, new ArrayList<ExecutorDetails>());
            }

            componentToExecutors.get(component).add(executor);
        }

        return componentToExecutors;
    }
}
