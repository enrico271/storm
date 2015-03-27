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
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.ksafety.DeduplicationBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoStreamWithoutKSafetyTopology {

    private static org.apache.log4j.Logger LOG;
    private static final int PRINT_INTERVAL = 5000;

    public static class ContinuousStreamSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;
        private ArrayList<String> names;

        public ContinuousStreamSpout()
        {
            names = new ArrayList<String>();
            names.add("A");
            names.add("B");
            names.add("C");
            names.add("D");
            names.add("E");
        }


        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }


        public void nextTuple() {

            Utils.sleep(100);

            org.apache.log4j.Logger.getLogger(ContinuousStreamSpout.class).info("Greetings, traveler!");

            for (String s : names) {
                _collector.emit(new Values(count, s, 1));
                count++;
            }
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "name", "value"));
        }

    }

    public static class DoublingBolt extends BaseRichBolt {

        OutputCollector _collector;
        int _deadTasks = 0;
        int _taskIndex;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "name", "value"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            _taskIndex = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple input) {
            org.apache.log4j.Logger.getLogger(DoublingBolt.class).info("Doubling " + input.toString());

            if (_taskIndex < _deadTasks) {
                return;
            }

            int v = input.getIntegerByField("value");
            _collector.emit(new Values(input.getIntegerByField("id"), input.getStringByField("name"), v * 2));
        }


        public void setDeadTasks(int n) {
            _deadTasks = n;
        }
    }

    public static class DedupBolt extends DeduplicationBolt {

        private HashMap<String,Integer> map = new HashMap<String,Integer>();

        public DedupBolt() {
            super("id");
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            new Thread() {
                @Override
                public void run(){
                    while (true)
                    {
                        Utils.sleep(PRINT_INTERVAL);
                        org.apache.log4j.Logger.getLogger(DedupBolt.class).info("----------- Capstone -------------");
                        for (String s : map.keySet())
                            org.apache.log4j.Logger.getLogger(DedupBolt.class).info(s + ": " + map.get(s));
                        map.clear();
                        clearKeys();
                    }

                }
            }.start();
        }


        @Override
        public void executeImpl(Tuple tuple) {

            String name = tuple.getStringByField("name");
            Integer value = tuple.getIntegerByField("value");

            if (map.containsKey(name))
                map.put(name, map.get(name) + value);
            else
                map.put(name, value);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id")); // unused
        }

    }

    public static class RegularBolt extends BaseRichBolt {

        private HashMap<String,Integer> map = new HashMap<String,Integer>();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            new Thread() {
                @Override
                public void run(){
                    org.apache.log4j.Logger.getLogger(RegularBolt.class).info("Thread started");
                    while (true)
                    {
                        Utils.sleep(PRINT_INTERVAL);
                        org.apache.log4j.Logger.getLogger(RegularBolt.class).info("---------- Capstone ---------- ");
                        for (String s : map.keySet())
                            org.apache.log4j.Logger.getLogger(RegularBolt.class).info(s + ": " + map.get(s));
                        map.clear();
                    }

                }
            }.start();
        }


        @Override
        public void execute(Tuple tuple) {

            String name = tuple.getStringByField("name");
            Integer value = tuple.getIntegerByField("value");

            if (map.containsKey(name))
                map.put(name, map.get(name) + value);
            else
                map.put(name, value);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id")); // unused
        }

    }

    public static void main(String[] args) throws Exception {

        LOG = org.apache.log4j.Logger.getLogger(DemoStreamWithoutKSafetyTopology.class);
        LOG.info("Hello world!");

        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new ContinuousStreamSpout(), 1);

        /*
         * First bolt
         */
        DoublingBolt bolt1 = new DoublingBolt();
        //bolt1.setDeadTasks(0);
        builder.setBolt("bolt1", bolt1, 2).fieldsGrouping("spout", new Fields("id"));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new RegularBolt(), 1).allGrouping("bolt1");



        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);


        if (args != null && args.length > 0) {
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


