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
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DemoServerTopologyNative {

    private static ServerSocket listener;
    private static int size = 1024;

    public static void startServer() {

        System.out.println("Starting server");

        try {

            listener = new ServerSocket(2727);
            while (true)
                new Connection(listener.accept()).start();


        } catch (IOException e) { e.printStackTrace(); }
        finally {
            try {
                listener.close();
            } catch (IOException e) {}
        }

    }

    private static class Connection extends Thread {
        private Socket socket;

        public Connection(Socket socket) {
            System.out.println("New connection: " + socket.toString());
            this.socket = socket;
        }

        public void run() {

            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg;
                while ((msg = in.readLine()) != null) {
                    size = Integer.parseInt(msg);
                }
            }
            catch (IOException e) { e.printStackTrace(); }
            finally {
                System.out.println("Closing connection: " + socket.toString());
                try { socket.close(); } catch (IOException e) {e.printStackTrace();}
            }
        }
    }


    public static class ServerSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;
        private String largeString = null;
        private int stringSize = 256;
        private long intervalStart = -1;
        private long intervalSizeSent = 0;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;

            StringBuffer str = new StringBuffer();
            for (int i = 0; i < stringSize; i++)
                str.append('a');
            largeString = str.toString();

            new Thread() {
                @Override
                public void run(){
                    startServer();
                }
            }.start();
        }


        public void nextTuple() {
            if (intervalSizeSent < size) {
                _collector.emit(new Values(count++, largeString, System.currentTimeMillis()));
                intervalSizeSent += stringSize;
            } else {
                long curTime = System.currentTimeMillis();
                long extra = 1000 - (curTime - intervalStart);
                System.out.println("Sent " + intervalSizeSent + " bytes with extra time " + extra);
                if (extra > 0) {
                    Utils.sleep(extra);
                }
                intervalStart = System.currentTimeMillis();
                intervalSizeSent = 0;
            }
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

    public static class SecondBolt extends BaseRichBolt {

        private int tuplesReceived = 0;
        private long lastPrint = 0;
        private long totalLatency = 0;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            lastPrint = System.currentTimeMillis();

            new Thread() {
                @Override
                public void run()
                {
                    while (true) {
                        if (System.currentTimeMillis() - lastPrint >= 1000) {
                            System.out.println("Throughput: " + tuplesReceived + " tuples/sec");
                            System.out.println("Average latency: " + ((double) totalLatency / tuplesReceived) + " ms");
                            lastPrint = System.currentTimeMillis();
                            tuplesReceived = 0;
                            totalLatency = 0;
                        }
                    }
                }
            }.start();
        }

        @Override
        public void execute(Tuple tuple) {
            tuplesReceived++;
            long stamp = tuple.getLongByField("timestamp");
            long latency = System.currentTimeMillis() - stamp;
            totalLatency += latency;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id"));
        }

    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new ServerSpout(), 1);

        /*
         * First bolt
         */
        DummyBolt bolt1 = new DummyBolt();
        builder.setBolt("bolt1", bolt1, 2).fieldsGrouping("spout", new Fields("id"));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new SecondBolt(), 1).allGrouping("bolt1");



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


