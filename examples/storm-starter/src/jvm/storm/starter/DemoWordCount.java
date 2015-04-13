/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.grouping.ksafety.KSafeFieldGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.ksafety.KSafeBolt;
import backtype.storm.topology.ksafety.KSafeSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoWordCount {

    private static final int COUNTING_BOLT_TASKS = 4;

    private static final int TYPE_WORD = 0;
    private static final int TYPE_EOF = 1;
    private static final int TYPE_MARK = 2;

    public static class WordSpout extends KSafeSpout {
        private String _text = null;
        private Scanner _scan = null;
        private boolean markSent = false;

        @Override
        public void openImpl(Map conf, TopologyContext context) {

            _text = "Jianneng Ashkon Enrico Zhitao Michael\n" +
                    "Ashkon Zhitao Jianneng Enrico Michael\n" +
                    "Zhitao Jianneng Michael Enrico Ashkon\n" +
                    "Enrico Ashkon Zhitao Michael Jianneng\n" +
                    "Michael Ashkon Zhitao Jianneng Enrico\n" +
                    "Jianneng Ashkon Enrico Zhitao Michael\n" +
                    "Ashkon Michael Jianneng Enrico Zhitao\n" +
                    "Zhitao Jianneng Michael Enrico Ashkon\n" +
                    "Enrico Ashkon Zhitao Michael Jianneng\n" +
                    "Michael Ashkon Zhitao Jianneng Enrico";
            _scan = new Scanner(_text);
        }

        @Override
        public void nextTuple() {

            Utils.sleep(100);

            if (!markSent) {
                // WARNING: This is a hack to send mark to all tasks
                for (int i = 0; i < COUNTING_BOLT_TASKS; i++)
                    emit(new Values("" + i, TYPE_MARK));

                markSent = true;
            }
            else if (_scan.hasNext())
                emit(new Values(_scan.next(), TYPE_WORD));
            else {
                // WARNING: This is a hack to send EOF to all tasks
                for (int i = 0; i < COUNTING_BOLT_TASKS; i++)
                    emit(new Values("" + i, TYPE_EOF));

                _scan = new Scanner(_text);
                markSent = false;
            }
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("word", "type");
        }

    }

    public static class CountingBolt extends KSafeBolt {

        private int _deadTasks = 0;
        private int _taskIndex;

        //private HashMap<String, Integer> _countMap = new HashMap<String, Integer>();
        //private boolean marked = false;

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("countMap", "taskId");
        }

        @Override
        public void prepareImpl(Map stormConf, TopologyContext context) {
            _taskIndex = context.getThisTaskIndex();
            System.out.println("SETTING TASK " + _taskIndex);
        }


        @Override
        public void executeImpl(Tuple input) {

            if (_taskIndex < _deadTasks) {
                return;
            }

            int type = input.getIntegerByField("type");
            String word = input.getStringByField("word");

            switch (type) {

                case TYPE_WORD:

                    // Get the state
                    @SuppressWarnings("unchecked")
                    HashMap<String, Integer> countMap = (HashMap<String, Integer>) getState(input, "countMap");
                    if (countMap == null)
                        countMap = new HashMap<>();

                    // Modify the state
                    if (countMap.containsKey(word))
                        countMap.put(word, countMap.get(word) + 1);
                    else
                        countMap.put(word, 1);

                    // Save the state
                    setState(input, "countMap", countMap);

                    break;

                case TYPE_EOF:
                    Boolean marked = (Boolean) getState(input, "marked");

                    if (marked != null && marked) {
                        emit(input, new Values(getState(input, "countMap"), _taskIndex));
                        setState(input, "countMap", new HashMap<String, Integer>());
                    }

                    setState(input, "marked", false);

                    break;

                case TYPE_MARK:
                    setState(input, "marked", true);
                    break;

                default:
                    org.apache.log4j.Logger.getLogger(CountingBolt.class).info("ERROR: Received tuple of unknown type");
                    break;
            }
        }


        public void setDeadTasks(int n) {
            _deadTasks = n;
        }
    }

    public static class DedupBolt extends KSafeBolt {

        @Override
        protected void prepareImpl(Map stormConf, TopologyContext context) {
            new Thread() {
                @Override
                public void run(){
                    while (true) {
                        Utils.sleep(2000);
                        System.out.println("...");
                    }
                }
            }.start();
        }

        @Override
        public void executeImpl(Tuple tuple) {

            @SuppressWarnings("unchecked")
            HashMap<String, Integer> countMap = (HashMap<String,Integer>) tuple.getValueByField("countMap");

            System.out.println("--- Word Count from task " + tuple.getIntegerByField("taskId") + ") ---");
            for (String s : countMap.keySet())
                System.out.println(s + ": " + countMap.get(s));
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("timestamp"); // unused
        }

    }


    public static void main(String[] args) throws Exception {

        int k = 1;

        if (args.length >= 1) {
            k = Integer.parseInt(args[0]);
            org.apache.log4j.Logger.getLogger(DemoWordCount.class).info("Hello world! Starting topology with k = " + k);
        }
        else {
            org.apache.log4j.Logger.getLogger(DemoWordCount.class).info("Hello world! No argument is found. Starting topology with k = " + k);
        }


        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new WordSpout(), 2);

        /*
         * First bolt
         */
        CountingBolt bolt1 = new CountingBolt();
        bolt1.setDeadTasks(0);
        builder.setBolt("bolt1", bolt1, COUNTING_BOLT_TASKS).customGrouping("spout", new KSafeFieldGrouping(k));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new DedupBolt(), 2).customGrouping("bolt1", new KSafeFieldGrouping(k));


        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);


        if (args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ksafe", conf, builder.createTopology());

        }
    }


}


