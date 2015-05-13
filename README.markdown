#*K*-Safe Storm

This repository contains a *k*-safe version of Storm that allows *k* machine failures to have no impact on performance. This was a capstone project for UC Berkeley Master of Engineering program.

----------

###Installation

First, clone the repository:
```
git clone https://github.com/enrico271/storm.git
```

Next, edit `storm/scripts/deploy.sh` and add the location of your SSH key (for cluster access) there. Change this line:
```
SSH_KEY=<location of your key>
```

Do the same thing for `storm/scripts/ssh.sh`.

Now you are ready to deploy! Simply run the script to compile the source code and deploy it to the cluster:
```
./storm/scripts/deploy.sh
```

> **Note:**
> This script will call `stormdistribute` in the cluster, which is a script that helps distributing Storm to all of the machines.

###Running an example program

First, SSH to the cluster:
```
./storm/scripts/ssh.sh
```

Let's run a word count topology to test if *k*-safety is working, but before that, let's make sure that we are using the correct scheduler.

Open the Storm UI by entering this address in your browser:
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

If everything is good, let's run the word count program! Run the following commands to start the word count topology with *k*=1:
```
cd apache-storm-0.9.4
./bin/storm jar examples/storm-starter/storm-starter-topologies-0.9.4.jar storm.starter.KSafeWordCount wordcount 1
```
> **Note:**
> The number of *k* is determined by the last argument. Therefore, the command above sets *k* to 1.

To see the output, we need to locate the final bolt because that bolt prints the output to the console. Run the following command to see the location of spouts and bolts:
```
loc
```
> **Note:**
> `loc` is a script that we made to help us find spouts and bolts

We are interested in `bolt2`, so if for example `bolt2` is located in `storm02`, then we need to SSH to `storm02`. For the purpose of this tutorial, let's assume that `bolt2` is in `storm02`. We now need to SSH to `storm02`:
```
ssh storm02
```

Now let's see the output! It is located in `apache-storm-0.9.4/logs/`, so go to that directory and find the log. The file name of the log may change. For example, suppose that the file name is `worker-6700.log`:
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

Now, locate the machines that contain `bolt1` using `loc` again. You can shut down one of these machines, and the output should not change. It shows that *k*-safety is working correctly. Awesome!

You can repeat this experiment with *k*=0, and you can see that the output will no longer be correct if you shut down a machine.