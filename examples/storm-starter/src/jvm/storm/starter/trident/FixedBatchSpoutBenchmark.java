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
package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;


public class FixedBatchSpoutBenchmark implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;
    private String _text = null;
    private Scanner _scan = null;
    HashMap<Long, List<String>> batches = new HashMap<>();
    HashMap<Long, Long> timestamps = new HashMap<Long, Long>();
    private Socket socket;
    private BufferedReader in;
    private Socket socket2;
    private PrintWriter out;
    private int stringSize = 256;
    private String[] letters = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t" ,"u" ,"v","w","x","y","z"};


    public FixedBatchSpoutBenchmark(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }
    
    int index = 0;
    boolean cycle = false;
    
    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }
    
    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
        StringBuilder str = new StringBuilder();
        for(int k =0; k < 10; k++) {
            for (int i = 0; i < 26; i++) {
                String curChar = letters[i];
                str.append(curChar);
                for (int j = 0; j < stringSize - 1; j++)
                    str.append("a");
                str.append(" ");
            }
        }
        _text = str.toString();
        _scan = new Scanner(_text);


        try {
            socket = new Socket("storm00", 2222);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) { e.printStackTrace(); }

        try {
            socket2 = new Socket("storm00", 3333);
            out = new PrintWriter(socket2.getOutputStream(), true);
        } catch (IOException e) { e.printStackTrace(); }


    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        Long time = 0L;
        try {
            String msg = in.readLine();
            if (msg != null) {
                time = Long.parseLong(msg.substring(0, 13));
                timestamps.put(batchId, time);
            }
            else{
                return;
            }
        }
        catch (IOException e) { e.printStackTrace(); }

        List<String> batch = this.batches.get(batchId);
        if(batch == null){
            batch = new ArrayList<String>();
            for(int i=0; i < maxBatchSize; i++) {
                if(!_scan.hasNext()){
                    _scan = new Scanner(_text);
                }

                batch.add(_scan.next());
            }
            this.batches.put(batchId, batch);
        }
        for(String str : batch){
            collector.emit(new Values(time, str));
        }
    }

    @Override
    public void ack(long batchId) {
        //System.out.println("WE HAVE ACKED THE BATCH: " + Long.toString(batchId));
        this.batches.remove(batchId);
        if(timestamps.containsKey(batchId)){
            Long stamp = timestamps.get(batchId);
            out.println(stamp);
            out.flush();
            timestamps.remove(batchId);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
    
}