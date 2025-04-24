package oafp.topology;

import oafp.spout.SentenceSpout;
import oafp.bolt.WordSplitterBolt;
import oafp.bolt.WordCounterBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static TopologyBuilder build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentence-spout", new SentenceSpout(), 3);
        builder.setBolt("splitter", new WordSplitterBolt(), 4).shuffleGrouping("sentence-spout");
        builder.setBolt("counter", new WordCounterBolt(), 5).fieldsGrouping("splitter", new Fields("word"));

        return builder;
    }
}
