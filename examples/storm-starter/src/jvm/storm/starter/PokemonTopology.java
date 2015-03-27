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
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.ksafety.DeduplicationBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class PokemonTopology {

    /*
     * Each task of this spout will output 50 Pokemons
     * Pokemon = 50% Pikachu and 50% Bulbasaur
     */
    public static class RandomPokemonSpout extends BaseRichSpout {
        boolean _isDistributed;
        SpoutOutputCollector _collector;
        private int count = 0;

        public RandomPokemonSpout() {
            this(true);
        }

        public RandomPokemonSpout(boolean isDistributed) {
            _isDistributed = isDistributed;
        }

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        public void close() {

        }

        public void nextTuple() {
            if (count >= 50)
                return;
            count++;

            Utils.sleep(100);
            final String[] words = new String[] {"pikachu", "bulbasaur"};
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];

             _collector.emit(new Values(count, word));


        }

        public void ack(Object msgId) {

        }

        public void fail(Object msgId) {

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if(!_isDistributed) {
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            } else {
                return null;
            }
        }
    }

    public static class PokemonCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(tuple.getIntegerByField("id"), word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "word", "count"));
        }
    }

    public static class PokemonCombineCountDedup extends DeduplicationBolt {

        private OutputCollector _collector;
        private Map<String, Integer> counts = new HashMap<String, Integer>();

        public PokemonCombineCountDedup(String field) {
            super(field);
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void executeImpl(Tuple tuple) {
            String word = tuple.getStringByField("word");
            Integer count = tuple.getIntegerByField("count");
            counts.put(word, count);

            _collector.emit(new Values(counts.keySet(), counts.values()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("words", "counts"));
        }
    }

    public static class PokemonCombineCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            Integer count = tuple.getIntegerByField("count");
            counts.put(word, count);

            collector.emit(new Values(counts.keySet(), counts.values()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("words", "counts"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomPokemonSpout(), 1); // 100 total Pokemons
        // TODO: Scheduling: What if #tasks more than #machines?
        builder.setBolt("count", new PokemonCount(), 2).customGrouping("spout", new KSafeFieldGrouping(1));
        builder.setBolt("combine", new PokemonCombineCountDedup("id"), 1).shuffleGrouping("count");
        //builder.setBolt("combine", new PokemonCombineCount(), 1).shuffleGrouping("count");
        /*
         * Note: the correct output should be [[bulbasaur, pikachu], [x,y]] where x + y = 50
         */

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ksafe", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }


}


