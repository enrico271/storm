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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoWordCountBenchmark {

    private static final int COUNTING_BOLT_TASKS = 2;

    private static final int TYPE_WORD = 0;
    private static final int TYPE_EOF = 1;
    private static final int TYPE_MARK = 2;

    public static class WordSpout extends KSafeSpout {
        private String _text = null;
        private Scanner _scan = null;
        private boolean markSent = false;
        private Socket socket;
        private BufferedReader in;

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

            try {
                socket = new Socket("localhost", 2222);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            } catch (IOException e) { e.printStackTrace(); }
        }

        @Override
        public void nextTuple() {

            Utils.sleep(100);

            if (!markSent) {
                // WARNING: This is a hack to send mark to all tasks
                for (int i = 0; i < COUNTING_BOLT_TASKS; i++)
                    emit(new Values("" + i, TYPE_MARK, 0L));

                markSent = true;
            }
            else if (_scan.hasNext())

                try {
                    String msg = in.readLine();
                    if (msg != null) {
                        Long time = Long.parseLong(msg.substring(0, 13));
                        emit(new Values(_scan.next(), TYPE_WORD, time));
                    }
                }
                catch (IOException e) { e.printStackTrace(); }

            else {
                // WARNING: This is a hack to send EOF to all tasks
                for (int i = 0; i < COUNTING_BOLT_TASKS; i++)
                    emit(new Values("" + i, TYPE_EOF, 0L));

                _scan = new Scanner(_text);
                markSent = false;
            }
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("word", "type", "time");
        }

    }

    public static class CountingBolt extends KSafeBolt {

        private int _deadTasks = 0;
        private int _taskIndex;

        //private HashMap<String, Integer> _countMap = new HashMap<String, Integer>();
        //private boolean marked = false;

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("word", "count", "taskId", "time");
        }

        @Override
        public void prepareImpl(Map stormConf, TopologyContext context) {
            _taskIndex = context.getThisTaskIndex();
        }


        @Override
        public void executeImpl(Tuple input) {

            if (_taskIndex < _deadTasks) {
                return;
            }

            int type = input.getIntegerByField("type");
            String word = input.getStringByField("word");
            Long time = input.getLongByField("time");

            switch (type) {

                case TYPE_WORD:
                    int count;
                    // Get the state
                    @SuppressWarnings("unchecked")
                    HashMap<String, Integer> countMap = (HashMap<String, Integer>) getState(input, "countMap");
                    if (countMap == null)
                        countMap = new HashMap<>();

                    // Modify the state
                    if (countMap.containsKey(word)) {
                        count = countMap.get(word) + 1;
                        countMap.put(word, count);
                    }
                    else {
                        count = 1;
                        countMap.put(word, 1);
                    }
                    emit(input,new Values(word, count, _taskIndex, time ));

                    // Save the state
                    setState(input, "countMap", countMap);

                    break;

                case TYPE_EOF:

                    Boolean marked = (Boolean) getState(input, "marked");

                    if (marked != null && marked) {
                       // emit(input, new Values(getState(input, "countMap"), _taskIndex));
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

        private Socket socket;
        private PrintWriter out;

        @Override
        protected void prepareImpl(Map stormConf, TopologyContext context) {
            try {
                socket = new Socket("localhost", 3333);
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) { e.printStackTrace(); }
        }

        @Override
        public void executeImpl(Tuple tuple) {

            @SuppressWarnings("unchecked")
            String word = tuple.getStringByField("word");
            String count = tuple.getIntegerByField("count").toString();
            Long time = tuple.getLongByField("time");
            //HashMap<String, Integer> countMap = (HashMap<String,Integer>) tuple.getValueByField("countMap");
            System.out.println(word + ": " + count);
            out.println(time);
            out.flush();
            //System.out.println("--- Word Count from task " + tuple.getIntegerByField("taskId") + ") ---");
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
            org.apache.log4j.Logger.getLogger(DemoWordCountBenchmark.class).info("Hello world! Starting topology with k = " + k);
        }
        else {
            org.apache.log4j.Logger.getLogger(DemoWordCountBenchmark.class).info("Hello world! No argument is found. Starting topology with no k-safety.");
        }


        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new WordSpout(), 1);

        /*
         * First bolt
         */
        CountingBolt bolt1 = new CountingBolt();
        bolt1.setDeadTasks(0);
        builder.setBolt("bolt1", bolt1, COUNTING_BOLT_TASKS).customGrouping("spout", new KSafeFieldGrouping(k));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new DedupBolt(), 1).customGrouping("bolt1", new KSafeFieldGrouping(0));


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


