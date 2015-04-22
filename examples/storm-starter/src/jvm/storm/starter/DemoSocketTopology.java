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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Read input from socket, and output final result to socket, with k-safety.
 */
public class DemoSocketTopology {
    public static class ServerSpout extends KSafeSpout {
        private int count = 0;
        private String largeString = null;
        private int stringSize = 256;
        private Socket socket;
        private BufferedReader in;

        @Override
        public void openImpl(Map conf, TopologyContext context) {
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
                    emit(new Values(count++, largeString, time));
                }
            }
            catch (IOException e) { e.printStackTrace(); }
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("id", "msg", "timestamp");
        }
    }

    public static class DummyBolt extends KSafeBolt {

        public DummyBolt(int k) {
            super(k);
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("id", "msg", "timestamp");
        }

        @Override
        public void prepareImpl(Map stormConf, TopologyContext context) {
        }

        @Override
        public void executeImpl(Tuple input) {
            emit(input, new Values(input.getIntegerByField("id"), input.getStringByField("msg"), input.getLongByField("timestamp")));
        }
    }

    public static class FinalBolt extends KSafeBolt {
        private Socket socket;
        private PrintWriter out;

        public FinalBolt(int k) {
            super(k);
        }

        @Override
        public void prepareImpl(Map stormConf, TopologyContext context) {
            try {
                socket = new Socket("storm00", 3333);
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) { e.printStackTrace(); }
        }

        @Override
        public void executeImpl(Tuple tuple) {
            long stamp = tuple.getLongByField("timestamp");
            out.println(stamp);
            out.flush();
        }

        @Override
        public Fields declareOutputFieldsImpl() {
            return new Fields("id");
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("-----------------------------------------------------------------------------");
            System.out.println("Usage: DemoSocketTopology <topology_name> <k> <topology_structure>");
            System.out.println("  topology_name: name to use when submitting this topology");
            System.out.println("  k: how many k for k-safety");
            System.out.println("  topology_structure: how many spouts, bolts, e.g.: \"1 2 1\" or \"2 4 6 8 2\"");
            System.out.println("-----------------------------------------------------------------------------");
            System.exit(1);
        }

        int k = Integer.parseInt(args[1]);

        /*
         * Spout
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ServerSpout(), Integer.parseInt(args[2]));

        /*
         * Intermediate bolts
         */
        for (int i = 3; i < args.length - 1; i++) {
            if (i - 3 <= 0)
                builder.setBolt("bolt" + (i-2), new DummyBolt(k), Integer.parseInt(args[i])).customGrouping("spout", new KSafeFieldGrouping(k));
            else
                builder.setBolt("bolt" + (i-2), new DummyBolt(k), Integer.parseInt(args[i])).customGrouping("bolt" + (i-3), new KSafeFieldGrouping(k));
        }

        /*
         * Final bolt
         */
        if (args.length - 4 <= 0)
            builder.setBolt("bolt" + (args.length - 3), new FinalBolt(k), Integer.parseInt(args[args.length - 1])).customGrouping("spout", new KSafeFieldGrouping(k));
        else
            builder.setBolt("bolt" + (args.length - 3), new FinalBolt(k), Integer.parseInt(args[args.length - 1])).customGrouping("bolt" + (args.length - 4), new KSafeFieldGrouping(k));


        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ksafe", conf, builder.createTopology());
        }
    }
}
