package oafp.topology;

import oafp.bolt.MatcherBolt;
import oafp.bolt.ParserBolt;
import oafp.spout.SentenceSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class GrepTopology {
    public static TopologyBuilder build() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new SentenceSpout(), 1);
        builder.setBolt("parser-bolt", new ParserBolt(), 2).shuffleGrouping("sentence-spout");
        builder.setBolt("matcher-bolt", new MatcherBolt(), 2).fieldsGrouping("parser-bolt", new Fields("word"));
        return builder;
    }
}