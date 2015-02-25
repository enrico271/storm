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
import backtype.storm.grouping.KSafeFieldGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.DeduplicationBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoServerTopology {


    public static class ServerSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;

        public ServerSpout()
        {
            new Thread() {
                @Override
                public void run(){
                    DemoServer.startServer();
                }
            }.start();
        }


        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }


        public void nextTuple() {

            //_collector.emit(new Values(count++));
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "msg"));
        }

    }

    public static class DoublingBolt extends BaseRichBolt {

        OutputCollector _collector;
        int _deadTasks = 0;
        int _taskIndex;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "value"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            _taskIndex = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple input) {
            if (_taskIndex < _deadTasks) {
                System.out.println("Ignoring tuple: " + input.toString());
                return;
            }

            int v = input.getIntegerByField("value");
            _collector.emit(new Values(input.getStringByField("id"), v * 2));
        }


        public void setDeadTasks(int n) {
            _deadTasks = n;
        }
    }

    public static class DedupBolt extends DeduplicationBolt {
        ArrayList<Tuple> arr = new ArrayList<Tuple>();

        public DedupBolt(String field) {
            super(field);
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void executeImpl(Tuple tuple) {
            arr.add(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "value", "final"));
        }

    }

    public static class RegularBolt extends BaseBasicBolt {
        ArrayList<Tuple> arr = new ArrayList<Tuple>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            arr.add(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "value", "final"));
        }

    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new ServerSpout(), 1);

//        /*
//         * First bolt
//         */
//        DoublingBolt bolt1 = new DoublingBolt();
//        bolt1.setDeadTasks(0);
//        builder.setBolt("bolt1", bolt1, 2).customGrouping("spout", new KSafeFieldGrouping(1));
//
//        /*
//         * Second bolt
//         */
//        final boolean DEDUP = true;
//        if (DEDUP)
//            builder.setBolt("bolt2", new DedupBolt("id"), 1).allGrouping("bolt1");
//        else
//            builder.setBolt("bolt2", new RegularBolt(), 1).allGrouping("bolt1");



        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, true);


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


