package oafp.topology;

import oafp.bolt.*;
import oafp.spout.SentenceSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class G3Topology {
    public static TopologyBuilder build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("A", new SentenceSpout());
        builder.setBolt("B", new KeywordFilterBolt()).shuffleGrouping("A");
        builder.setBolt("C", new ClassifierBolt()).shuffleGrouping("A");

        builder.setBolt("D", new JoinerBolt())
                .fieldsGrouping("B", new Fields("word"))
                .fieldsGrouping("C", new Fields("category"));

        builder.setBolt("E", new CountBolt()).shuffleGrouping("A");

        builder.setBolt("F", new ResultBolt())
                .shuffleGrouping("D")
                .shuffleGrouping("E");

        return builder;
    }
}
