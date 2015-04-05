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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.ksafety.DeduplicationBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class BenchmarkTopology {

    private static final int TUPLE_SIZE = 256;
    private static final int ONE_MB = 1024000;
    private static final int[] BANDWIDTHS = {ONE_MB, 2* ONE_MB, 4 * ONE_MB, 8 * ONE_MB, 16 * ONE_MB, 32 * ONE_MB};
    //private static final int[] BANDWIDTHS = {16 * ONE_MB, 32 * ONE_MB};
    private static final int[] EXPECTED_TUPLES = new int[BANDWIDTHS.length];
    private static final int PHASE_DURATION_SEC = 10;
    private static final String DONE = "done";
    private static final int PORT = 6789;

    public static class ServerSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        private int count = 0;
        private String largeString = null;
        private long intervalStart = -1;
        private long intervalSizeSent = 0;
        private int phase = 0;
        private int tuplesSent = 0;
        private boolean waitForNextPhase = false;
        private BufferedReader in = null;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;

            StringBuffer str = new StringBuffer();
            for (int i = 0; i < TUPLE_SIZE; i++)
                str.append('a');
            largeString = str.toString();

            try {
                ServerSocket socket = new ServerSocket(PORT);
                in = new BufferedReader(new InputStreamReader(socket.accept().getInputStream()));
                System.out.println("Spout connection OK");
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            while (true) {
                                String msg = in.readLine();
                                if (msg.equals(DONE)) {
                                    System.out.println("Received a message indicating that final bolt has processed all tuples");
                                    tuplesSent = 0;
                                    phase++;
                                    if (phase < BANDWIDTHS.length) {
                                        System.out.println("Starting phase " + phase);
                                        waitForNextPhase = false;
                                    }
                                    else {
                                        System.out.println("Benchmark finished");
                                    }
                                }
                                else {
                                    System.out.println("Error: Unknown message");
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void nextTuple() {

            while (waitForNextPhase) {
                Utils.sleep(1000);
            }

            if (intervalSizeSent < BANDWIDTHS[phase]) {
                tuplesSent++;
                if (tuplesSent == EXPECTED_TUPLES[phase])
                    waitForNextPhase = true;
                _collector.emit(new Values(count++, largeString, System.currentTimeMillis()));
                intervalSizeSent += TUPLE_SIZE;
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

            _collector.emit(input.getValues());
        }
    }

    public static class FinalBolt extends BaseRichBolt {

        private int tuplesReceived = 0;
        private long startTime = 0;
        private long totalLatency = 0;
        private int phase = 0;
        private PrintWriter out = null;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            try {
                out = new PrintWriter(new Socket("localhost", PORT).getOutputStream(), true);
                System.out.println("Final bolt connection OK");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public void execute(Tuple tuple) {
            long stamp = tuple.getLongByField("timestamp");
            long latency = System.currentTimeMillis() - stamp;
            totalLatency += latency;

            if (tuplesReceived == 0)
                startTime = System.currentTimeMillis();

            tuplesReceived++;

            if (tuplesReceived == EXPECTED_TUPLES[phase]) {
                long totalTime = System.currentTimeMillis() - startTime;
                double avgBandwidth = (double) (tuplesReceived * TUPLE_SIZE / ONE_MB) / (totalTime / 1000);
                double avgLatency = (double) totalLatency / tuplesReceived;
                // double avgLatency = (double) totalTime / tuplesReceived; // this one is not 100% accurate, but it allows final bolt and spout to be on different machines

                System.out.println("===== Phase " + phase + " finished! =====");
                System.out.println("Total time: " + (double) totalTime / 1000 + " sec");
                System.out.println("Expected bandwidth: " + (double) BANDWIDTHS[phase] / ONE_MB + " MB/s");
                System.out.println("Avg bandwidth: " + avgBandwidth + " MB/s");
                System.out.println("Avg latency: " + avgLatency + " ms.");

                // Reset vars
                tuplesReceived = 0;
                totalLatency = 0;
                phase++;
                out.println(DONE);
                if (phase < BANDWIDTHS.length)
                    System.out.println("Sent a request to begin phase " + phase);
                else
                    System.out.println("Benchmark finished");
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id"));
        }

    }


    public static void main(String[] args) throws Exception {

        for (int i = 0; i < BANDWIDTHS.length; i++) {
            EXPECTED_TUPLES[i] = BANDWIDTHS[i] / TUPLE_SIZE * PHASE_DURATION_SEC;
            System.out.println("Setting phase " + i + ", expected tuples: " + EXPECTED_TUPLES[i]);
        }

        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Spout
         */
        builder.setSpout("spout", new ServerSpout(), 1);

        /*
         * First bolt
         */
        DummyBolt bolt1 = new DummyBolt();
        //builder.setBolt("bolt1", bolt1, 2).customGrouping("spout", new KSafeFieldGrouping(0));
        builder.setBolt("bolt1", bolt1, 2).fieldsGrouping("spout", new Fields("msg"));

        /*
         * Second bolt
         */
        builder.setBolt("bolt2", new FinalBolt(), 1).allGrouping("bolt1");



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


