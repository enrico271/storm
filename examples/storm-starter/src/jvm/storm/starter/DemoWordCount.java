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
import backtype.storm.topology.ksafety.DeduplicationBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
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

    private static org.apache.log4j.Logger LOG;
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


        public void nextTuple() {

            Utils.sleep(100);

            if (!markSent) {
                // WARNING: This is a hack to send EOF to both tasks
                emit(new Values("a", System.currentTimeMillis(), TYPE_MARK));
                emit(new Values("b", System.currentTimeMillis(), TYPE_MARK));
                markSent = true;
            }
            else if (_scan.hasNext())
                emit(new Values(_scan.next(), System.currentTimeMillis(), TYPE_WORD));
            else {
                // WARNING: This is a hack to send EOF to both tasks
                emit(new Values("a", System.currentTimeMillis(), TYPE_EOF));
                emit(new Values("b", System.currentTimeMillis(), TYPE_EOF));
                _scan = new Scanner(_text);
                markSent = false;
            }
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("word", "timestamp", "type");
        }

    }

    public static class CountingBolt extends BaseRichBolt {

        private OutputCollector _collector;
        private int _deadTasks = 0;
        private int _taskIndex;

        private HashMap<String, Integer> _countMap = new HashMap<String, Integer>();
        private Long _minTime = null;

        private boolean marked = false;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("timestamp", "countMap", "taskId"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            _taskIndex = context.getThisTaskIndex();
            System.out.println("Number of tasks for this bolt: " + context.getComponentTasks(context.getThisComponentId()).size());
        }


        @Override
        public void execute(Tuple input) {

            if (_taskIndex < _deadTasks) {
                return;
            }

            long timestamp = input.getLongByField("timestamp");
            int type = input.getIntegerByField("type");
            String word = input.getStringByField("word");

            switch (type) {
                case TYPE_WORD:
                    if (_minTime == null || timestamp < _minTime)
                        _minTime = timestamp;
                    if (_countMap.containsKey(word))
                        _countMap.put(word, _countMap.get(word) + 1);
                    else
                        _countMap.put(word, 1);
                    break;

                case TYPE_EOF:
                    if (marked) {
                        _collector.emit(new Values(_minTime, _countMap, _taskIndex));
                        _countMap = new HashMap<String, Integer>();
                    }
                    _minTime = null;
                    marked = false;
                    break;

                case TYPE_MARK:
                    marked = true;
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

    public static class DedupBolt extends DeduplicationBolt {

        public DedupBolt() {
            super("timestamp");
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        @Override
        public void executeImpl(Tuple tuple) {

            HashMap<String, Integer> countMap = (HashMap<String,Integer>) tuple.getValueByField("countMap");

            org.apache.log4j.Logger.getLogger(DedupBolt.class).info("----------- Capstone (Word Count from task " + tuple.getIntegerByField("taskId") + ") -------------");
            for (String s : countMap.keySet())
                org.apache.log4j.Logger.getLogger(DedupBolt.class).info(s + ": " + countMap.get(s));

            // WARNING: This implementation never clears the hash set inside DeduplicationBolt
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("timestamp")); // unused
        }

    }


    public static void main(String[] args) throws Exception {

        int k = 0;

        if (args.length >= 1) {
            k = Integer.parseInt(args[0]);
            org.apache.log4j.Logger.getLogger(DemoWordCount.class).info("Hello world! Starting topology with k = " + k);
        }
        else {
            k = 0;
            org.apache.log4j.Logger.getLogger(DemoWordCount.class).info("Hello world! No argument is found. Starting topology with no k-safety.");
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
        //bolt1.setDeadTasks(1);
        builder.setBolt("bolt1", bolt1, 2).customGrouping("spout", new KSafeFieldGrouping(k));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new DedupBolt(), 1).allGrouping("bolt1");


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


