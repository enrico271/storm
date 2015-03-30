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
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.ksafety.DeduplicationBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoTopology {

    public static void printTuplesToFile(List<Tuple> tuples) {

        try {
            PrintWriter writer = new PrintWriter("storm-output.txt", "UTF-8");

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            writer.println(dateFormat.format(date));

            for (Tuple t : tuples)
                writer.println(t.getValues().toString());

            writer.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class SimpleSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;
        private ArrayList<Values> values;

        public SimpleSpout ()
        {
            values = new ArrayList<Values>();
            values.add(new Values("A", 1));
            values.add(new Values("B", 2));
            values.add(new Values("C", 3));
            values.add(new Values("D", 4));
            values.add(new Values("E", 5));
        }


        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }


        public void nextTuple() {

            Utils.sleep(100);

            if (count < values.size())
                _collector.emit(values.get(count));
            count++;
        }


        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "value"));
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

        @Override
        public void cleanup() {
            super.cleanup();
            printTuplesToFile(arr);
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

        @Override
        public void cleanup() {
            super.cleanup();
            printTuplesToFile(arr);
        }
    }

    public static void main(String[] args) throws Exception {

        org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(DemoTopology.class);
        LOG.info("Hello world");

        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new SimpleSpout(), 1);

        /*
         * First bolt
         */
        DoublingBolt bolt1 = new DoublingBolt();
        bolt1.setDeadTasks(0);
        builder.setBolt("bolt1", bolt1, 2).customGrouping("spout", new KSafeFieldGrouping(1));

        /*
         * Second bolt
         */
        final boolean DEDUP = true;
        if (DEDUP)
            builder.setBolt("bolt2", new DedupBolt("id"), 1).allGrouping("bolt1");
        else
            builder.setBolt("bolt2", new RegularBolt(), 1).allGrouping("bolt1");


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

            Thread.sleep(5000);

            cluster.shutdown();
        }
    }


}


