#*K*-Safe Storm

This repository contains a *k*-safe version of Storm that allows *k* machine failures to have no impact on performance. This was a capstone project for UC Berkeley Master of Engineering program.

----------

###Installation

1. Clone the repository.
    ```
    git clone https://github.com/enrico271/storm.git
    ```

2. Edit `storm/scripts/deploy.sh` and add the location of your SSH key (for cluster access) there. Change this line:
    ```
    SSH_KEY=<location of your key>
    ```

    Do the same thing for `storm/scripts/ssh.sh`.

3. Now you are ready to deploy! Simply run the script to compile the source code and deploy it to the cluster:
    ```
    ./storm/scripts/deploy.sh
    ```

    > **Note:**
    > This script will call `stormdistribute` in the cluster, which is a script that helps distributing Storm to all of the machines.


----------

###Running an example program

We will run a simple word count topology to test whether *k*-safety is working. The topology looks like the following figure:
```
Figure 1. A simple topology

       /--- bolt1 ---\
      /               \
spout                  bolt2
      \               /
       \--- bolt1 ---/
```
With this topology, we can test *k*-safety with *k*=1. That is, if we shut down one of the two `bolt1`s, the topology should still be running normally because *k*=1 should tolerate one machine failure. The source (`spout`) and the final node (`bolt2`) are single points of failure. However, they can be made *k*-safe too. For the sake of this discussion, we assume that they are always up to make this tutorial simpler.

1. First, SSH to the cluster:
    ```
    ./storm/scripts/ssh.sh
    ```

2. Let's make sure that we are using the correct scheduler. Open the Storm UI by entering this address in your browser:
    ```
    http://localhost:9999/
    ```

    Make sure that the scheduler is set to `KSafeScheduler`:
    ```
    storm.scheduler    storm.KSafeScheduler
    ```

    > **Note:**
    > The scheduler can be changed in `storm/conf/defaults.yaml` by changing this line:
    > ```
    > storm.scheduler: "storm.KSafeScheduler"
    > ``` 

3. If everything is good, let's run the word count program! Run the following commands to start the word count topology with *k*=1:
    ```
    cd apache-storm-0.9.4
    ./bin/storm jar examples/storm-starter/storm-starter-topologies-0.9.4.jar storm.starter.KSafeWordCount wordcount 1
    ```
    > **Note:**
    > The number of *k* is determined by the last argument. Therefore, the command above sets *k* to 1.

4. To see the output, we need to locate the final bolt because that bolt prints the output to the console. Run the following command to see the location of spouts and bolts:
    ```
    loc
    ```
    > **Note:**
    > `loc` is a script that we made to help us find spouts and bolts

5. We are interested in `bolt2`, so if for example `bolt2` is located in `storm02`, then we need to SSH to `storm02`. For the purpose of this tutorial, let's assume that `bolt2` is in `storm02`. We now need to SSH to `storm02`:
    ```
    ssh storm02
    ```

6. Now let's see the output! It is located in `apache-storm-0.9.4/logs/`, so go to that directory and find the log. The file name of the log may change. For example, if the file name is `worker-6700.log`, then we run the commands below:
    ```
    cd apache-storm-0.9.4/logs/
    tail -f worker-6700.log
    ```

    You should see that it continously prints some word count. The correct output is (order doesn't matter):
    ```
    Jianneng: 10
    Enrico: 10
    Ashkon: 10
    Zhitao: 10
    Michael: 10
    ```

7. Now, locate the machines that contain `bolt1` using `loc` again. You can shut down one of these machines, and the output should not change. It shows that *k*-safety is working correctly. Awesome!

8. You can repeat this experiment with *k*=0, and you can see that the output will no longer be correct if you shut down a machine.


----------

###Running the benchmark
>**TODO:** Explain how to change the scheduler to MyCombinedScheduler. Probably give this scheduler a better name as well.

1. SSH to `storm00` and go to the home directory.
2. We have provided a script to conveniently run the benchmark. To use the script, use this command:
    ```
    ./benchmark.sh <class name> <topology name> <topology arguments ...>
    ```
    - `<class name>`: the system that we want to benchmark. It should be one of these three:
        - `storm.starter.BenchmarkKsafe`
        - `storm.starter.BenchmarkStormWithAcking`
        - `storm.starter.BenchmarkStormWithoutAcking`
    
    - `<topology name>`: the name of the topology to display in the Storm UI. If the topology name is `storm`, our scheduler will switch back to the Storm default scheduler.
    - `<topology arguments ...>`: the arguments that will be used to structure the topology.
        - For `BenchmarkKsafe`, the first argument is *k*, the rest is topology structure.
        - For `BenchmarkStormWithAcking` and `BenchmarkStormWithoutAcking`, all arguments are the topology structure.

        For example, to run `BenchmarkKsafe` with the topology pictured in Figure 1 above and *k*=1, run this command:
        ```
        ./benchmark.sh storm.starter.BenchmarkKsafe ksafe 1 1 2 1
        ```
    The first argument is `1` to set *k*=1, the rest is `1 2 1`, which says we want 1 spout, 2 middle bolts, and 1 final bolt.
    > **Note:**
    > We can make the topology as long as we want. For example, the argument `2 2 4 5 6 2 1` is also valid.