package oafp;

import oafp.topology.GrepTopology;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.Config;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = GrepTopology.build();
        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("GrepTopology", config, builder.createTopology());
    }
}