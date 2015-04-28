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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Read input from socket, and output final result to socket, without k-safety and without acking.
 */
public class DemoSocketTopologyNative2 {
    public static class ServerSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;
        private String largeString = null;
        private int stringSize = 256;
        private Socket socket;
        private BufferedReader in;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;

            StringBuilder str = new StringBuilder();
            for (int i = 0; i < stringSize; i++)
                str.append('a');
            largeString = str.toString();

            try {
                socket = new Socket("storm00", 2222);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            } catch (IOException e) { e.printStackTrace(); }
        }

        public void nextTuple() {
            try {
                String msg = in.readLine();
                if (msg != null) {
                    Long time = Long.parseLong(msg.substring(0, 13));
                    _collector.emit(new Values(count++, largeString, time));
                }
            }
            catch (IOException e) { e.printStackTrace(); }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "msg", "timestamp"));
        }
    }

    public static class DummyBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "msg", "timestamp"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            _collector.emit(new Values(input.getIntegerByField("id"), input.getStringByField("msg"), input.getLongByField("timestamp")));
        }
    }

    public static class FinalBolt extends BaseRichBolt {
        private Socket socket;
        private PrintWriter out;
        OutputCollector _collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            try {
                socket = new Socket("storm00", 3333);
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) { e.printStackTrace(); }
        }

        @Override
        public void execute(Tuple tuple) {
            long stamp = tuple.getLongByField("timestamp");
            out.println(stamp);
            out.flush();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id"));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("-----------------------------------------------------------------------------");
            System.out.println("Usage: DemoSocketTopologyNative2 <topology_name> <topology_structure>");
            System.out.println("  topology_name: name to use when submitting this topology");
            System.out.println("  topology_structure: how many spouts, bolts, e.g.: \"1 2 1\" or \"2 4 6 8 2\"");
            System.out.println("-----------------------------------------------------------------------------");
            System.exit(1);
        }

        /*
         * Spout
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ServerSpout(), Integer.parseInt(args[1]));

        /*
         * Intermediate bolts
         */
        for (int i = 2; i < args.length - 1; i++) {
            if (i - 2 <= 0)
                builder.setBolt("bolt" + (i-1), new DummyBolt(), Integer.parseInt(args[i])).fieldsGrouping("spout", new Fields("id"));
            else
                builder.setBolt("bolt" + (i-1), new DummyBolt(), Integer.parseInt(args[i])).fieldsGrouping("bolt" + (i - 2), new Fields("id"));
        }

        /*
         * Final bolt
         */
        if (args.length - 3 <= 0)
            builder.setBolt("bolt" + (args.length - 2), new FinalBolt(), Integer.parseInt(args[args.length - 1])).fieldsGrouping("spout", new Fields("id"));
        else
            builder.setBolt("bolt" + (args.length - 2), new FinalBolt(), Integer.parseInt(args[args.length - 1])).fieldsGrouping("bolt" + (args.length - 3), new Fields("id"));

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(24);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ksafe", conf, builder.createTopology());
        }
    }
}
