package oafp.bolt;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import oafp.experiment.RecoveryLogger;

import java.util.Map;

public class ResultBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String output = input.toString();
        System.out.println("[ResultBolt 收到输出] → " + input.toString());
        RecoveryLogger.log(output);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
