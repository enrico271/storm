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
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.*;
import storm.trident.util.TridentUtils;
import backtype.storm.utils.Utils;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.starter.trident.FixedBatchSpoutBenchmark;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.HashMap;


public class TridentTest {
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }
  public static class PrintTuple extends BaseFilter {
      @Override
      public boolean isKeep(TridentTuple tuple) {
          //System.out.println(tuple);
          return true;
      }
  }

    public static class BetterCount implements CombinerAggregator<Long> {

        @Override
        public Long init(TridentTuple tuple) {
            return 1L;
        }

        @Override
        public Long combine(Long val1, Long val2) {
            return val1 + val2;
        }

        @Override
        public Long zero() {
            return 0L;
        }

    }

    public static class WordAggregator extends BaseAggregator<Map<String, Integer>> {

        @Override
        public Map<String, Integer> init(Object batchId, TridentCollector collector) {
            return new HashMap<String, Integer>();
        }

        @Override
        public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
            String word = tuple.getString(0);
            val.put(word, MapUtils.getInteger(val, word, 0) + 1);
        }

        @Override
        public void complete(Map<String, Integer> val, TridentCollector collector) {
            collector.emit(new Values(val));
        }
    }




  public static StormTopology buildTopology(int batchSize) {
      /*
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
      */
      FixedBatchSpoutBenchmark spout = new FixedBatchSpoutBenchmark(new Fields("time", "word"), batchSize, new Values("Jianneng"),
              new Values("Enrico"), new Values("Ashkon"), new Values("Zhitao"), new Values("Michael"));
    spout.setCycle(true);

    TridentTopology topology = new TridentTopology();
    //TridentState wordCounts =
    /*
    topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
    new Split(), new Fields("word")).groupBy(new Fields("word")).aggregate(new Fields("word"), new Count(), new Fields("count"))
    .each(new Fields("word", "count"), new PrintTuple());
    */

      /*
    topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
    new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
    .newValuesStream().each(new Fields("word", "count"), new PrintTuple());
    */
      topology.newStream("spout1", spout).parallelismHint(4).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(4)
      .newValuesStream().each(new Fields("word", "count"), new PrintTuple()).parallelismHint(4);

      //groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
           // new BetterCount(), new Fields("count")).parallelismHint(16);
            //.each(new Fields("word", "count"), new PrintTuple(), new Fields("word", "count"));
    /*
    topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
        "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
        new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
    */
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    int batch_size;
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(10000));
     // for (int i = 0; i < 100; i++) {
      //  System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
      //  Thread.sleep(1000);
     // }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar("storm", conf, buildTopology(Integer.parseInt(args[0])));
    }
  }
}
